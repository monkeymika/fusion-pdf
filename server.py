import os
import io
import tempfile
import requests
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pypdf import PdfReader, PdfWriter

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import uvicorn


app = FastAPI(title="Fusion PDF + Signets (Streaming)")


# ========= Téléchargement robuste (stream + headers + retries) =========
def fetch_pdf_stream_to_file(url: str, timeout: int = 300, chunk_size: int = 512 * 1024):
    """
    Télécharge un PDF en streaming dans un fichier temporaire.
    - Headers 'navigateur' + Referer (souvent requis par les flipbooks)
    - Retries sur erreurs réseau/5xx
    - Timeout large
    - Logs de progression
    """
    origin = f"{urlsplit(url).scheme}://{urlsplit(url).netloc}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,*/*;q=0.8",
        "Referer": origin,
    }

    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
        raise_on_status=False,
    )
    sess = requests.Session()
    sess.mount("http://", HTTPAdapter(max_retries=retry))
    sess.mount("https://", HTTPAdapter(max_retries=retry))

    # HEAD pour log taille indicative si dispo (ne bloque pas en cas d'échec)
    try:
        h = sess.head(url, headers=headers, timeout=30, allow_redirects=True)
        size = int(h.headers.get("Content-Length", 0))
        if size:
            print(f"[head] {url} size={size/1024/1024:.1f} MB", flush=True)
    except Exception:
        pass

    try:
        print(f"[fetch] START {url}", flush=True)
        with sess.get(url, stream=True, headers=headers, timeout=timeout) as r:
            r.raise_for_status()
            # 50 Mo en RAM puis déborde sur disque si besoin (faible empreinte mémoire)
            f = tempfile.SpooledTemporaryFile(max_size=50 * 1024 * 1024, mode="w+b")

            downloaded = 0
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    # log toutes les ~10 chunks
                    if downloaded % (10 * chunk_size) == 0:
                        print(f"[fetch] {url} ~{downloaded/1024/1024:.1f} MB", flush=True)

            f.seek(0)
            print(f"[fetch] DONE  {url} total ~{downloaded/1024/1024:.1f} MB", flush=True)
            return f

    except Exception as e:
        print(f"[fetch] ERROR {url} -> {e}", flush=True)
        raise HTTPException(status_code=400, detail=f"Erreur téléchargement PDF: {e}")


# ========= Middleware de logs (on voit toutes les requêtes arriver/partir) =========
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


# ========= Endpoints santé / probe =========
@app.get("/")
def health():
    return {"ok": True, "service": "fusion-pdf"}

@app.get("/fusion-pdf")
def fusion_pdf_get_probe():
    return {"ok": True, "hint": "Use POST /fusion-pdf with JSON body"}


# ========= Endpoint principal : fusion + signets =========
@app.post("/fusion-pdf")
def fusion_pdf(payload: dict):
    """
    payload attendu :
    {
      "catalogues": [
        { "fournisseur": "Azurlign", "url": "https://.../azurlign.pdf", "chapitres": [] },
        { "fournisseur": "CEDAM",    "url": "https://.../cedam.pdf",    "chapitres": [] },
        { "fournisseur": "Elios",    "url": "https://.../elios.pdf",    "chapitres": [] }
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

        # Catégories connues (utile pour la vue par catégories si tu ajoutes des chapitres plus tard)
        categories_connues = ["carrelage", "robinetterie", "meuble", "sanitaire", "autre"]
        bookmarks_par_categorie = {c: [] for c in categories_connues}

        # Signet racine
        racine = writer.add_outline_item(titre_global, 0)

        temp_files = []  # pour fermer proprement à la fin
        try:
            for cat in catalogues:
                try:
                    fournisseur = cat["fournisseur"]
                    pdf_url = cat["url"]
                    chapitres = cat.get("chapitres", [])

                    print(f"[merge] + {fournisseur} | {pdf_url}", flush=True)
                    fobj = fetch_pdf_stream_to_file(pdf_url)
                    temp_files.append(fobj)

                    reader = PdfReader(fobj)
                    print(f"[merge] {fournisseur} pages={len(reader.pages)} offset={page_offset}", flush=True)

                    # Signet fournisseur
                    bm_fournisseur = writer.add_outline_item(f"📁 {fournisseur}", page_offset, parent=racine)

                    # Empiler pages
                    for page in reader.pages:
                        writer.add_page(page)

                    # Chapitres internes (optionnels, si fournis)
                    for ch in chapitres:
                        try:
                            titre = ch["titre"]
                            categorie = ch.get("categorie", "autre").lower()
                            debut = max(1, int(ch["page_debut"])) - 1  # 0-based
                            page_absolue = page_offset + debut

                            writer.add_outline_item(f"• {titre}", page_absolue, parent=bm_fournisseur)

                            cible = categorie if categorie in bookmarks_par_categorie else "autre"
                            bookmarks_par_categorie[cible].append(
                                {"titre": f"{fournisseur} - {titre}", "page": page_absolue}
                            )
                        except Exception:
                            # Ne casse pas la fusion si un chapitre est mal renseigné
                            pass

                    page_offset += len(reader.pages)

                except Exception as e:
                    # On loggue précisément quel fournisseur/URL a posé problème
                    print(
                        f"[error] fournisseur={cat.get('fournisseur')} url={cat.get('url')} -> {type(e).__name__}: {e}",
                        flush=True,
                    )
                    raise HTTPException(status_code=400, detail=f"{cat.get('fournisseur')} | {e}")

            # Vue par catégorie (bonus prêt pour la suite)
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
        # On propage les HTTPException avec leurs détails
        raise
    except Exception as e:
        # File au client un message clair pour tout autre crash
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
