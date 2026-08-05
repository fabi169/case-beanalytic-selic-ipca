"""Configurações centralizadas do pipeline SELIC/IPCA."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Nomes de catálogo, schema, volume e caminhos usados no pipeline."""

    catalog: str = "analise_taxas"
    schema: str = "selic_ipca"
    volume: str = "dados_brutos"

    # Volume dedicado para checkpoints/schema location do Auto Loader.
    # Não pode ser o DBFS root (desabilitado por padrão em workspaces mais
    # novos / Free Edition) nem o mesmo Volume dos dados brutos (causaria
    # erro LOCATION_OVERLAP, pois o checkpoint ficaria dentro do storage
    # gerenciado do próprio volume de dados).
    checkpoint_volume: str = "checkpoints"

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def checkpoint_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.checkpoint_volume}"

    def full_table(self, table_name: str) -> str:
        return f"{self.catalog}.{self.schema}.{table_name}"


DEFAULT_CONFIG = PipelineConfig()
