from src.utils.cli_output import print_header, print_json_kv, print_kv, print_mapping


def test_cli_output_helpers_emit_expected_text(capsys):
    print_header('Demo')
    print_kv('Status', 'ok')
    print_mapping('Metrics', {'sharpe': 1.23456, 'trades': 3})
    print_json_kv('Params', {'a': 1})

    out = capsys.readouterr().out
    assert '=== Demo ===' in out
    assert 'Status: ok' in out
    assert 'Metrics:' in out
    assert 'sharpe: 1.2346' in out
    assert 'trades: 3' in out
    assert 'Params: {"a": 1}' in out
