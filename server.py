from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pypdf import PdfReader, PdfWriter
import io, os, requests, tempfile
import uvicorn

app = FastAPI(title="Fusion PDF + Signets (Streaming)")

def fetch_pdf_stream_to_file(url: str, timeout: int = 60, chunk_size: int = 1024 * 1024):
    """
    Télécharge un PDF en streaming dans un fichier temporaire.
    Utilise SpooledTemporaryFile : stocke en mémoire jusqu’à un seuil,
    puis déborde automatiquement sur disque (faible empreinte RAM).
    Retourne un objet fichier prêt pour PdfReader.
    """
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            # 50 Mo en RAM puis bascule sur disque si > 50 Mo
            f = tempfile.SpooledTemporaryFile(max_size=50 * 1024 * 1024, mode="w+b")
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:  # éviter les keep-alive chunks
                    f.write(chunk)
            f.seek(0)
            return f
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur téléchargement PDF: {url} -> {e}")

@app.post("/fusion-pdf")
def fusion_pdf(payload: dict):
    """
    payload attendu (exemple simple sans chapitres internes) :
    {
      "catalogues": [
        { "fournisseur": "Azurlign", "url": "https://exemple.com/azurlign.pdf", "chapitres": [] },
        { "fournisseur": "CEDAM",    "url": "https://exemple.com/cedam.pdf",    "chapitres": [] },
        { "fournisseur": "Elios",    "url": "https://exemple.com/elios.pdf",    "chapitres": [] }
      ],
      "titre_global": "Catalogues 2025 - Test Fusion"
    }
    """
    try:
        catalogues = payload.get("catalogues", [])
        if not catalogues:
            raise ValueError("Aucun catalogue fourni.")

        titre_global = payload.get("titre_global", "Catalogue fusionné")
        writer = PdfWriter()
        page_offset = 0

        # Catégories connues (on garde pour la suite même si chapitres vides)
        categories_connues = ["carrelage", "robinetterie", "meuble", "sanitaire", "autre"]
        bookmarks_par_categorie = {c: [] for c in categories_connues}

        # Signet racine
        racine = writer.add_outline_item(titre_global, 0)

        # Boucle fournisseurs (streaming fichier par fichier)
        temp_files = []  # on garde des références pour fermer proprement
        try:
            for cat in catalogues:
                fournisseur = cat["fournisseur"]
                pdf_url = cat["url"]
                chapitres = cat.get("chapitres", [])

                # Téléchargement en streaming (faible RAM)
                fobj = fetch_pdf_stream_to_file(pdf_url)
                temp_files.append(fobj)

                reader = PdfReader(fobj)

                # Signet fournisseur
                bm_fournisseur = writer.add_outline_item(f"📁 {fournisseur}", page_offset, parent=racine)

                # Empiler pages
                for page in reader.pages:
                    writer.add_page(page)

                # Chapitres (si tu en ajoutes plus tard)
                for ch in chapitres:
                    try:
                        titre = ch["titre"]
                        categorie = ch.get("categorie", "autre").lower()
                        debut = max(1, int(ch["page_debut"])) - 1  # 0-based
                        page_absolue = page_offset + debut
                        writer.add_outline_item(f"• {titre}", page_absolue, parent=bm_fournisseur)

                        cible = categorie if categorie in bookmarks_par_categorie else "autre"
                        bookmarks_par_categorie[cible].append({"titre": f"{fournisseur} - {titre}", "page": page_absolue})
                    except Exception:
                        # On n'échoue pas la fusion pour un chapitre mal formé
                        pass

                page_offset += len(reader.pages)

            # Vue par catégorie (bonus prêt pour l'IA plus tard)
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
            return StreamingResponse(
                buf,
                media_type="application/pdf",
                headers={"Content-Disposition": 'attachment; filename="catalogues_fusionnes.pdf"'}
            )

        finally:
            # Toujours fermer les fichiers temporaires
            for f in temp_files:
                try:
                    f.close()
                except Exception:
                    pass

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# 1) Endpoint santé (réveil + test rapide depuis le navigateur)
@app.get("/")
def health():
    return {"ok": True, "service": "fusion-pdf"}

# 2) Logs des requêtes (on voit chaque appel entrer/sortir)
@app.middleware("http")
async def log_requests(request, call_next):
    try:
        print(f"[req] {request.method} {request.url.path}", flush=True)
        response = await call_next(request)
        print(f"[res] {request.method} {request.url.path} -> {response.status_code}", flush=True)
        return response
    except Exception as e:
        print(f"[err] {request.method} {request.url.path} -> {e}", flush=True)
        raise

# 3) GET de probe pour /fusion-pdf (juste pour vérifier le routage)
@app.get("/fusion-pdf")
def fusion_pdf_get_probe():
    return {"ok": True, "hint": "Use POST /fusion-pdf with JSON body"}



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
