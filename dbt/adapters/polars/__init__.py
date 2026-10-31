from dbt.adapters.base.plugin import AdapterPlugin
from dbt.exceptions import ModelConfigError
from dbt_common.dataclass_schema import ValidationError
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.connections import PolarsCredentials
from dbt.adapters.polars.impl import PolarsAdapter
from dbt.include.polars import PACKAGE_PATH

# Two dbt-core monkeypatches to support Python singular tests (dbt-core 1.11.x):
#
# 1. get_file_types_for_project: adds ".py" to ParseFileType.SingularTest extensions
#    so Python test files are discovered by the parser.
#
# 2. SingularTestParser.render_update: runs parse_python_model for .py test nodes so
#    dbt.ref() calls are statically scanned and added to depends_on. Without this, dbt's
#    runtime ref() validation rejects any dbt.ref() call in a .py test. Requires a
#    "def model" entry point (satisfied by _TEST_ENTRYPOINT_WRAPPER below).
_TEST_ENTRYPOINT_WRAPPER = "\n\n\ndef model(dbt, pl):\n    return test(dbt, pl)\n"

try:
    import dbt.parser.read_files as _read_files
    from dbt.node_types import ModelLanguage
    from dbt.parser.models import ModelParser, verify_python_model_code
    from dbt.parser.singular_test import SingularTestParser

    _original_get_file_types_for_project = _read_files.get_file_types_for_project

    def _get_file_types_for_project_with_py_tests(project):
        file_types = _original_get_file_types_for_project(project)
        try:
            file_types[_read_files.ParseFileType.SingularTest]["extensions"].append(
                ".py"
            )
        except (KeyError, AttributeError, TypeError):
            pass
        return file_types

    _read_files.get_file_types_for_project = _get_file_types_for_project_with_py_tests

    _original_singular_test_render_update = SingularTestParser.render_update

    def _singular_test_render_update(
        self, node, config, validate_config_call_dict=False
    ):
        if node.language == ModelLanguage.python:
            if "def test(" not in node.raw_code:
                raise DbtRuntimeError(
                    f"Python singular test '{node.name}' must define "
                    + "a function named 'test(dbt, pl)'"
                )
            if not node.raw_code.endswith(_TEST_ENTRYPOINT_WRAPPER):
                node.raw_code += _TEST_ENTRYPOINT_WRAPPER
            try:
                verify_python_model_code(node)
                context = self._context_for(node, config)
                ModelParser.parse_python_model(self, node, config, context)
                self.update_parsed_node_config(
                    node, config, context=context, validate_config_call_dict=True
                )
            except ValidationError as exc:
                raise ModelConfigError(exc, node=node) from exc
            except Exception:
                return _original_singular_test_render_update(
                    self, node, config, validate_config_call_dict
                )
            return
        return _original_singular_test_render_update(
            self, node, config, validate_config_call_dict
        )

    SingularTestParser.render_update = _singular_test_render_update  # type: ignore[method-assign]
except (ImportError, AttributeError):
    from dbt.adapters.events.logging import AdapterLogger

    AdapterLogger("polars").warning(
        "Python singular tests are not supported with this version of dbt-core."
    )

# dbt-core monkeypatch to support unit tests on Python models:
#
# UnitTestManifestLoader.parse_unit_test_case builds the unit test node from the tested
# model's code as if it were SQL: refs are captured by rendering it with Jinja and
# no Python postfix is added. For a Python model, the node gets the model's refs,
# sources and config, and code that renders to the model plus a Python postfix
# for unit tests. Compiling the postfix calls ref() for each input, which injects
# the fixtures.
try:
    from dbt.contracts.graph.model_config import UnitTestNodeConfig
    from dbt.node_types import ModelLanguage
    from dbt.parser.unit_tests import UnitTestManifestLoader

    original_parse_unit_test_case = UnitTestManifestLoader.parse_unit_test_case

    def parse_unit_test_case_for_python_models(self, test_case):
        original_parse_unit_test_case(self, test_case)
        tested_node = self.manifest.nodes[test_case.depends_on.nodes[0]]
        if tested_node.language != ModelLanguage.python:
            return
        unit_test_node = self.unit_test_manifest.nodes[test_case.unique_id]
        unit_test_node.refs = tested_node.refs
        unit_test_node.sources = tested_node.sources
        unit_test_node.config = UnitTestNodeConfig.from_dict(
            {
                **tested_node.config.to_dict(),
                "materialized": "unit",
                "expected_rows": unit_test_node.config.expected_rows,
                "expected_sql": unit_test_node.config.expected_sql,
            }
        )
        # Python model code has no Jinja (verify_python_model_code), so rendering
        # it leaves it unchanged
        unit_test_node.raw_code = (
            tested_node.raw_code + "\n\n{{ polars_unit_test_python_postfix(model) }}"
        )

    UnitTestManifestLoader.parse_unit_test_case = (  # type: ignore[method-assign]
        parse_unit_test_case_for_python_models
    )
except (ImportError, AttributeError):
    from dbt.adapters.events.logging import AdapterLogger

    AdapterLogger("polars").warning(
        "Unit tests on Python models are not supported with this version of dbt-core."
    )

Plugin = AdapterPlugin(
    adapter=PolarsAdapter,  # type: ignore[arg-type]
    credentials=PolarsCredentials,
    include_path=PACKAGE_PATH,
)
