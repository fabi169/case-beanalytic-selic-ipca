"""Upload automático de arquivos locais para um Volume do Unity Catalog,
usando o Databricks SDK (Files API) em vez do CLI manual.

Requer autenticação já configurada (o mesmo perfil usado por `databricks
configure` / `~/.databrickscfg`) e o pacote `databricks-sdk` instalado:
    pip install databricks-sdk
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def subir_arquivo_para_volume(caminho_local: Path, caminho_volume: str) -> None:
    """Envia um arquivo local para um caminho dentro de um Volume do Unity Catalog.

    Args:
        caminho_local: caminho do arquivo no disco local.
        caminho_volume: caminho de destino, no formato
            "/Volumes/<catalog>/<schema>/<volume>/<subpasta>/<arquivo>".

    Raises:
        FileNotFoundError: se o arquivo local não existir.
        RuntimeError: se o upload falhar por qualquer motivo (rede,
            autenticação, permissão).
    """
    if not caminho_local.exists():
        raise FileNotFoundError(f"Arquivo local não encontrado: {caminho_local}")

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as erro:
        raise RuntimeError(
            "Pacote 'databricks-sdk' não instalado. Rode: "
            "pip install databricks-sdk"
        ) from erro

    try:
        cliente = WorkspaceClient()
        with open(caminho_local, "rb") as arquivo:
            cliente.files.upload(caminho_volume, arquivo, overwrite=True)
        logger.info(f"Upload concluído: {caminho_local} -> {caminho_volume}")
    except Exception as erro:
        raise RuntimeError(
            f"Falha ao subir {caminho_local} para {caminho_volume}: {erro}"
        ) from erro
