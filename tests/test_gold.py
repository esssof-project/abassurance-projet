
import pytest

from spark import streaming_pipeline as pipeline


@pytest.mark.parametrize(
    "builder",
    [
        pipeline.build_clients,
        pipeline.build_contrats,
        pipeline.build_sinistres,
        pipeline.build_paiements,
    ]
)
def test_gold_builders(spark_session, builder):
    result = builder(spark_session)

    if result is None:
        pytest.skip(f"Aucune donnée Bronze pour {builder.__name__}")

    assert len(result.columns) > 0

    assert result.limit(1).count() > 0


def test_build_doublons(spark_session):
    clients = pipeline.build_clients(spark_session)

    if clients is None or clients.limit(1).count() == 0:
        pytest.skip("Aucun client disponible")

    result = pipeline.build_doublons(clients)

    if result is None:
        pytest.skip("Résultat doublons indisponible")

    expected_columns = [
        "id_ab",
        "id_ap",
        "nom",
        "prenom",
        "date_naissance",
        "code_postal",
    ]

    assert result.columns == expected_columns
