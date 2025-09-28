import pandas as pd

from aurum.parsers.schema_inference import SchemaInferenceEngine


def test_schema_inference_maps_synonyms():
    frame = pd.DataFrame({
        "tenor": ["2024-01"],
        "price": [42.0],
        "curve": ["abc"],
    })

    engine = SchemaInferenceEngine()
    result = engine.infer(frame)

    assert result.column_mapping["tenor"] == "tenor_label"
    assert result.column_mapping["price"] == "mid"
    assert result.confidence > 0.0
