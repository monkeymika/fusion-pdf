import os
import io
import tempfile
from urllib.parse import urlsplit

import requests
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import StreamingResponse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pypdf import PdfReader, PdfWriter
import uvicorn

app = FastAPI(title="Fusion PDF (pypdf)")

# --------- Download robuste vers fichier disque ----------
def download_pdf_to_tempfile(url: str, timeout: int = 600, chunk_size: int = 1024 * 1024) -> str:
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

    # HEAD (info taille si dispo)
    try:
        h = sess.head(url, headers=headers, timeout=30, allow_redirects=True)
        size = int(h.headers.get("Content-Length", 0))
        if size:
            print(f"[head] {url} size={size/1024/1024:.1f} MB", flush=True)
    except Exception:
        pass

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp_path = tmp.name
    try:
        downloaded = 0
        print(f"[fetch] START {url}", flush=True)
        with sess.get(url, stream=True, headers=headers, timeout=timeout) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (10 * chunk_size) == 0:
                        print(f"[fetch] {url} ~{downloaded/1024/1024:.1f} MB", flush=True)
        tmp.flush(); tmp.close()
        print(f"[fetch] DONE  {url} total ~{downloaded/1024/1024:.1f} MB", flush=True)
        return tmp_path
    except Exception as e:
        try:
            tmp.close()
        except:
            pass
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except:
            pass
        print(f"[fetch] ERROR {url} -> {e}", flush=True)
        raise HTTPException(status_code=400, detail=f"Erreur téléchargement PDF: {e}")

# --------- Logs ----------
@app.middleware("http")
async def log_requests(request, call_next):
    try:
        print(f"[req] {request.method} {request.url.path}", flush=True)
        res = await call_next(request)
        print(f"[res] {request.method} {request.url.path} -> {res.status_code}", flush=True)
        return res
    except Exception as e:
        print(f"[err] {request.method} {request.url.path} -> {e}", flush=True)
        raise

# --------- Santé ----------
@app.get("/")
def health():
    return {"ok": True, "service": "fusion-pdf"}

@app.head("/")
def health_head():
    return Response(status_code=200)

@app.get("/fusion-pdf")
def probe_get():
    return {"ok": True, "hint": "Use POST /fusion-pdf with JSON body"}

@app.head("/fusion-pdf")
def probe_head():
    return Response(status_code=200)

# --------- Fusion (pypdf) ----------
@app.post("/fusion-pdf")
def fusion_pdf(payload: dict):
    """
    payload :
    {
      "catalogues": [
        {"fournisseur":"CEDAM","url":"https://.../cedam.pdf","chapitres":[]},
        {"fournisseur":"Elios","url":"https://.../elios.pdf","chapitres":[]}
      ],
      "titre_global": "Test Fusion"
    }
    """
    try:
        catalogues = payload.get("catalogues", [])
        if not catalogues:
            raise ValueError("Aucun catalogue fourni.")

        titre_global = payload.get("titre_global", "Catalogue fusionné")

        # 1) Télécharger sur disque
        temp_paths = []
        meta = []  # (fournisseur, path, nb_pages)
        try:
            for c in catalogues:
                fournisseur = c["fournisseur"]
                url = c["url"]
                print(f"[merge] + {fournisseur} | {url}", flush=True)
                path = download_pdf_to_tempfile(url)
                reader = PdfReader(path)
                nb = len(reader.pages)
                meta.append((fournisseur, path, nb))
                temp_paths.append(path)

            # 2) Construire le PDF sortie
            writer = PdfWriter()
            page_offset = 0
            supplier_bookmarks = []

            for fournisseur, path, nb in meta:
                reader = PdfReader(path)
                for p in range(nb):
                    writer.add_page(reader.pages[p])
                supplier_bookmarks.append((f"📁 {fournisseur}", page_offset))
                print(f"[merge] {fournisseur} pages={nb} offset={page_offset}", flush=True)
                page_offset += nb

            # signets fournisseurs
            root = writer.add_outline_item(titre_global, 0)
            for title, offset in supplier_bookmarks:
                try:
                    writer.add_outline_item(title, offset, parent=root)
                except Exception as e:
                    print(f"[outline] skip {title}: {e}", flush=True)

            # 3) écrire en mémoire + Content-Length
            buf = io.BytesIO()
            writer.write(buf)
            size = buf.getbuffer().nbytes
            buf.seek(0)

            # nettoyage fichiers sources
            for p in temp_paths:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except:
                    pass

            return StreamingResponse(
                buf,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": 'attachment; filename="catalogues_fusionnes.pdf"',
                    "Content-Length": str(size)
                }
            )

        except:
            # en cas d'erreur → nettoyage
            for p in temp_paths:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except:
                    pass
            raise

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
