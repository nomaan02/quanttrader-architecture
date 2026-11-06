from shared.projectx import QuoteCache, normalize_hub_payload


def test_unwraps_envelope_and_folds_pascal_case():
    event = {"data": {"ContractId": "CON.F.US.EP", "LastPrice": 5000.25}, "action": 1}
    assert normalize_hub_payload(event) == {"contractId": "CON.F.US.EP", "lastPrice": 5000.25}


def test_handles_signalr_argument_lists_and_json_strings():
    assert normalize_hub_payload([{"Id": 7}]) == {"id": 7}
    assert normalize_hub_payload(["CON.F.US.EP", '{"Balance": 150000}']) == {"balance": 150000}


def test_leaves_flat_camel_case_and_acronyms_alone():
    assert normalize_hub_payload({"contractId": "X", "PNL": 3}) == {"contractId": "X", "PNL": 3}


def test_does_not_unwrap_a_field_that_is_merely_called_data():
    assert normalize_hub_payload({"data": {"a": 1}}) == {"data": {"a": 1}}


def test_quote_cache_merges_partial_updates():
    cache = QuoteCache()
    cache.update("X", {"lastPrice": 1.0, "bestBid": 0.9, "bestAsk": 1.1})
    merged = cache.update("X", {"lastPrice": 1.2, "bestBid": None})
    assert merged == {"lastPrice": 1.2, "bestBid": 0.9, "bestAsk": 1.1}
