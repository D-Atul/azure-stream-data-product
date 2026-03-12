from __future__ import annotations

import importlib


contract_module = importlib.import_module("src.contracts.output_contract")


def test_output_contract_module_imports():
    assert contract_module is not None


def test_output_contract_has_public_symbols():
    public_names = [name for name in dir(contract_module) if not name.startswith("_")]
    assert len(public_names) > 0


def test_output_contract_exposes_net_flow_related_definition():
    names = [name.lower() for name in dir(contract_module)]
    assert any("net" in name and "flow" in name for name in names)


def test_output_contract_exposes_user_metrics_related_definition():
    names = [name.lower() for name in dir(contract_module)]
    assert any("user" in name and "metric" in name for name in names)


def test_output_contract_exposes_channel_distribution_related_definition():
    names = [name.lower() for name in dir(contract_module)]
    assert any("channel" in name for name in names)


def test_output_contract_definitions_are_not_none():
    public_values = [
        getattr(contract_module, name)
        for name in dir(contract_module)
        if not name.startswith("_")
    ]
    assert any(value is not None for value in public_values)