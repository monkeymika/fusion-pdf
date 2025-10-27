from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pypdf import PdfReader, PdfWriter
import io, requests

app = FastAPI(title="Fusion PDF + Signets")

@app.post("/fusion-pdf")
def fusion_pdf(payload: dict):
    """
    payload attendu:
    {
      "catalogues": [
        {
          "fournisseur": "Azurlign",
          "url": "https://exemple.com/azurlign.pdf",
          "chapitres": [
            {"titre":"Carrelage mural", "categorie":"carrelage", "page_debut":12, "page_fin":45}
          ]
        },
        ...
      ],
      "titre_global": "Catalogues 2025 - Tous fournisseurs"
    }
    """
    try:
        catalogues = payload.get("catalogues", [])
        if not catalogues:
            raise ValueError("Aucun catalogue fourni.")
        writer = PdfWriter()
        page_offset = 0
        # Catégories connues; ajoute/édite selon tes besoins
        categories_connues = ["carrelage","robinetterie","meuble","sanitaire","autre"]
        bookmarks_par_categorie = {c: [] for c in categories_connues}

        # (facultatif) signet racine
        racine = writer.add_outline_item(payload.get("titre_global","Catalogue fusionné"), 0)

        for cat in catalogues:
            fournisseur = cat["fournisseur"]
            pdf_url = cat["url"]
            chapitres = cat.get("chapitres", [])

            # Télécharger le PDF source
            r = requests.get(pdf_url, timeout=60)
            r.raise_for_status()
            reader = PdfReader(io.BytesIO(r.content))

            # Ajouter signet fournisseur
            bm_fournisseur = writer.add_outline_item(f"📁 {fournisseur}", page_offset, parent=racine)

            # Empiler pages
            for page in reader.pages:
                writer.add_page(page)

            # Ajouter signets chapitres
            for ch in chapitres:
                titre = ch["titre"]
                categorie = ch.get("categorie", "autre").lower()
                debut = max(1, int(ch["page_debut"])) - 1  # 0-based
                page_absolue = page_offset + debut

                writer.add_outline_item(f"• {titre}", page_absolue, parent=bm_fournisseur)

                if categorie not in bookmarks_par_categorie:
                    bookmarks_par_categorie["autre"].append({"titre": f"{fournisseur} - {titre}", "page": page_absolue})
                else:
                    bookmarks_par_categorie[categorie].append({"titre": f"{fournisseur} - {titre}", "page": page_absolue})

            page_offset += len(reader.pages)

        # Vue par catégorie (bonus)
        cat_root = writer.add_outline_item("🗂️ Navigation par catégorie", 0, parent=racine)
        for categorie, items in bookmarks_par_categorie.items():
            if items:
                cat_item = writer.add_outline_item(categorie.capitalize(), items[0]["page"], parent=cat_root)
                for it in items:
                    writer.add_outline_item(f"• {it['titre']}", it["page"], parent=cat_item)

        # Écrire en mémoire et renvoyer
        buf = io.BytesIO()
        writer.write(buf)
        buf.seek(0)
        return StreamingResponse(buf, media_type="application/pdf", headers={
            "Content-Disposition": 'attachment; filename="catalogues_fusionnes.pdf"'
        })

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
