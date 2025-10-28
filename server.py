import os
import io
import tempfile
from urllib.parse import urlsplit

import requests
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import fitz  # PyMuPDF
import uvicorn

app = FastAPI(title="Fusion PDF + Signets (PyMuPDF)")

# ---------- Téléchargement robuste -> fichier disque ----------
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

# ---------- Logs ----------
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

# ---------- Santé ----------
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

# ---------- Fusion (PyMuPDF / low-RAM) ----------
@app.post("/fusion-pdf")
def fusion_pdf(payload: dict):
    """
    payload :
    {
      "catalogues": [
        {"fournisseur":"CEDAM","url":"https://.../cedam.pdf","chapitres":[]},
        {"fournisseur":"Elios Ceramica","url":"https://.../elios.pdf","chapitres":[]}
      ],
      "titre_global": "Test Fusion"
    }
    """
    try:
        catalogues = payload.get("catalogues", [])
        if not catalogues:
            raise ValueError("Aucun catalogue fourni.")

        titre_global = payload.get("titre_global", "Catalogue fusionné")

        temp_paths = []
        meta = []  # (fournisseur, path, page_count)
        try:
            # 1) Télécharger sur disque + compter pages
            for c in catalogues:
                fournisseur = c["fournisseur"]
                url = c["url"]
                print(f"[merge] + {fournisseur} | {url}", flush=True)
                path = download_pdf_to_tempfile(url)
                with fitz.open(path) as src:
                    nb = src.page_count
                temp_paths.append(path)
                meta.append((fournisseur, path, nb))

            # 2) Concat output en écrivant sur disque, mémoire faible
            tmp_out = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            out_path = tmp_out.name
            tmp_out.close()

            # Ouvre un doc de sortie vide
            out = fitz.open()

            page_offset = 0
            toc = []  # liste [ [level, title, page+1], ... ]
            for fournisseur, path, nb in meta:
                with fitz.open(path) as src:
                    out.insert_pdf(src)  # insertion directe, peu de RAM
                # signet (niveau 1) sur la première page de ce fournisseur
                toc.append([1, f"📁 {fournisseur}", page_offset + 1])
                print(f"[merge] {fournisseur} pages={nb} offset={page_offset}", flush=True)
                page_offset += nb

            # Applique la TOC (signets)
            if toc:
                out.set_toc(toc)

            # Sauvegarde optimisée
            out.save(out_path, deflate=True, garbage=3)
            out.close()

            # Sanity check
            try:
                with fitz.open(out_path):
                    pass
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"PDF généré invalide: {e}")

            # 3) Retourner le fichier et nettoyer
            def cleanup(paths):
                for p in paths:
                    try:
                        if os.path.exists(p):
                            os.remove(p)
                    except:
                        pass

            bg = BackgroundTask(cleanup, temp_paths + [out_path])
            return FileResponse(
                path=out_path,
                media_type="application/pdf",
                filename="catalogues_fusionnes.pdf",
                background=bg,
            )

        finally:
            # Si erreur avant FileResponse, nettoyer inputs
            if temp_paths:
                for p in temp_paths:
                    try:
                        if os.path.exists(p):
                            os.remove(p)
                    except:
                        pass

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
