"""
Script de extração local (fora do Databricks) das séries SELIC e IPCA
via API do Banco Central, salvando os retornos brutos em JSON.

Suporta backfill: por padrão busca 01/01/2020 a 31/12/2024, mas o
intervalo pode ser alterado via argumentos de linha de comando, para
reprocessar um período específico sem precisar editar o código.

Uso:
    python extrai_selic_ipca.py
    python extrai_selic_ipca.py --data-inicial 01/01/2025 --data-final 31/07/2026
    python extrai_selic_ipca.py --upload  # também sobe os arquivos pro Volume
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from upload_volume import subir_arquivo_para_volume

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATA_INICIAL_PADRAO = "01/01/2020"
DATA_FINAL_PADRAO = "31/12/2024"

SERIES_BCB = {
    "selic": 11,
    "ipca": 433,
}

# Caminho de destino no Volume do Unity Catalog, usado apenas se --upload
# for passado. Cada série vai para sua própria subpasta (mesma estrutura
# esperada por src/bronze.py: dados_brutos/<serie>/<serie>.json).
VOLUME_DESTINO = "/Volumes/analise_taxas/selic_ipca/dados_brutos"


def cria_sessao() -> requests.Session:
    config_retentativas = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
    )
    sessao = requests.Session()
    adaptador = HTTPAdapter(max_retries=config_retentativas)
    sessao.mount("http://", adaptador)
    sessao.mount("https://", adaptador)
    return sessao


def monta_urls(data_inicial: str, data_final: str) -> dict[str, str]:
    """Monta as URLs da API do BCB para o intervalo de datas informado."""
    return {
        taxa: (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie_id}/dados"
            f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
        )
        for taxa, serie_id in SERIES_BCB.items()
    }


def parse_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extração local de SELIC e IPCA (API do BCB), com suporte a backfill."
    )
    parser.add_argument(
        "--data-inicial",
        default=DATA_INICIAL_PADRAO,
        help=f"Data inicial no formato dd/mm/aaaa (padrão: {DATA_INICIAL_PADRAO})",
    )
    parser.add_argument(
        "--data-final",
        default=DATA_FINAL_PADRAO,
        help=f"Data final no formato dd/mm/aaaa (padrão: {DATA_FINAL_PADRAO})",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Após extrair com sucesso, sobe os arquivos automaticamente para o Volume "
        "via Databricks SDK (requer 'pip install databricks-sdk' e autenticação configurada).",
    )
    return parser.parse_args()


def busca_dados_selic_ipca(urls: dict, pasta_saida: Path) -> dict[str, Path]:
    """Busca as séries e salva os arquivos JSON brutos.

    Returns:
        Dicionário {nome_serie: caminho_do_arquivo} apenas para as séries
        que foram salvas com sucesso.
    """
    sessao = cria_sessao()
    arquivos_salvos: dict[str, Path] = {}
    houve_falha = False

    for taxa, url in urls.items():
        try:
            resposta = sessao.get(url, timeout=10)
        except requests.exceptions.ConnectionError as erro:
            logger.error(f"[{taxa}] Erro de conexão: {erro}")
            houve_falha = True
            continue
        except requests.exceptions.Timeout as erro:
            logger.error(f"[{taxa}] Timeout: {erro}")
            houve_falha = True
            continue
        except requests.exceptions.RequestException as erro:
            logger.error(f"[{taxa}] Erro inesperado na requisição: {erro}")
            houve_falha = True
            continue

        if resposta.status_code != 200:
            logger.error(f"[{taxa}] Falha definitiva após retentativas. Status: {resposta.status_code}")
            houve_falha = True
            continue

        try:
            dados = resposta.json()
        except json.JSONDecodeError as erro:
            logger.error(f"[{taxa}] Resposta não é um JSON válido: {erro}")
            houve_falha = True
            continue

        if not dados:
            logger.error(f"[{taxa}] API respondeu 200, mas o payload veio vazio.")
            houve_falha = True
            continue

        caminho_saida = pasta_saida / f"{taxa}.json"
        with open(caminho_saida, "w", encoding="utf-8") as arquivo:
            json.dump(dados, arquivo, indent=4, ensure_ascii=False)

        logger.info(f"[{taxa}] OK - {len(dados)} registros salvos em {caminho_saida}")
        arquivos_salvos[taxa] = caminho_saida

    if houve_falha:
        logger.error("Extração concluída com pelo menos uma falha.")

    return arquivos_salvos


def sobe_arquivos_para_volume(arquivos_salvos: dict[str, Path]) -> bool:
    """Sobe cada arquivo extraído para sua subpasta correspondente no Volume.

    Returns:
        True se todos os uploads tiveram sucesso, False caso algum falhe.
    """
    sucesso_total = True
    for taxa, caminho_local in arquivos_salvos.items():
        caminho_volume = f"{VOLUME_DESTINO}/{taxa}/{taxa}.json"
        try:
            subir_arquivo_para_volume(caminho_local, caminho_volume)
        except (FileNotFoundError, RuntimeError) as erro:
            logger.error(f"[{taxa}] Falha no upload: {erro}")
            sucesso_total = False
    return sucesso_total


def main() -> int:
    args = parse_argumentos()
    urls = monta_urls(args.data_inicial, args.data_final)

    pasta_saida = Path(__file__).resolve().parent / "dados_brutos"
    pasta_saida.mkdir(parents=True, exist_ok=True)

    arquivos_salvos = busca_dados_selic_ipca(urls, pasta_saida)

    if len(arquivos_salvos) < len(urls):
        logger.error("Extração FALHOU: nem todas as séries foram salvas.")
        return 1

    if args.upload:
        logger.info("Iniciando upload automático para o Volume...")
        if not sobe_arquivos_para_volume(arquivos_salvos):
            logger.error("Upload FALHOU para pelo menos uma série.")
            return 1
        logger.info("Upload concluído com sucesso para todas as séries.")

    logger.info("Extração concluída com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
