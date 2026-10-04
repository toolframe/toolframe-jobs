"""Public declaration guarantees independent from installed Core admission."""

from importlib.resources import files

from toolframe_jobs.plugin import SERVICE_NAME, plugin


def test_scoped_methods_never_publish_anonymous_mutation_or_internal_api():
    methods = plugin.services[SERVICE_NAME].methods
    assert all(not method.public_api for method in methods.values())
    assert methods["request_cancel"].required_scopes == ("jobs.cancel",)
    assert not methods["request_cancel"].read_only
    for name in ("get_job", "list_jobs"):
        assert methods[name].required_scopes == ("jobs.read",)
        assert methods[name].read_only
    for name in (
        "enqueue",
        "claim",
        "heartbeat",
        "save_checkpoint",
        "complete",
        "fail",
        "acknowledge_cancel",
    ):
        assert methods[name].audience == "internal"
        assert not methods[name].required_scopes
        assert methods[name].input_schema["additionalProperties"] is False


def test_only_read_methods_publish_mcp_and_all_assets_are_packaged():
    tools = [resource for resource in plugin.resources if resource.kind == "mcp.tool"]
    assert {resource.data["method"] for resource in tools} == {"list_jobs", "get_job"}
    for resource in tools:
        assert plugin.services[SERVICE_NAME].methods[resource.data["method"]].read_only
    assert files("toolframe_jobs").joinpath("docs/usage.md").is_file()
    assert plugin.store.revision == "v1"
    assert "DOUBLE PRECISION" in plugin.store.migrations[0].dialects["postgresql"]
