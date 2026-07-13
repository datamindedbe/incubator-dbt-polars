from dbt.adapters.base.plugin import AdapterPlugin
from dbt.adapters.polars.connections import PolarsCredentials
from dbt.adapters.polars.impl import PolarsAdapter
from dbt.exceptions import ModelConfigError
from dbt.include.polars import PACKAGE_PATH
from dbt_common.dataclass_schema import ValidationError

# dbt-core (as of 1.11.x, dbt/parser/read_files.py:get_file_types_for_project)
# only registers ".sql" as a valid extension for ParseFileType.SingularTest -- ".py"
# is allowed for Model but not SingularTest, with no config-level way to add it.
# This monkeypatch adds ".py" to the SingularTest extension list so Python singular
# tests (see dbt/include/polars/macros/tests.sql) can be discovered by the parser.
#
# This depends on an internal dbt-core function, pinned to dbt-core 1.11.x's exact
# shape: a dict of {ParseFileType: {"extensions": [...], ...}} with a mutable list.
# If upgrading dbt-core, re-verify get_file_types_for_project still has this shape
# before assuming this patch still applies.
_python_tests_patched = True

try:
    import dbt.parser.read_files as _read_files

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
except (ImportError, AttributeError):
    _python_tests_patched = False

# dbt-core populates `node.refs`/`node.sources` (and, downstream, `depends_on.nodes`)
# for Python *models* by statically AST-scanning their source for ref()/source()/
# config() calls -- ModelParser.parse_python_model (dbt/parser/models.py), invoked
# from ModelParser.render_update only when node.language == python. SingularTestParser
# never calls this, so a .py test node's `refs`/`depends_on.nodes` stay empty, which
# then makes dbt-core's *runtime* ref() validation (RuntimeRefResolver.validate,
# dbt/context/providers.py) reject any dbt.ref(...) call the test's compiled code
# makes ("dbt was unable to infer all dependencies...").
#
# This monkeypatch makes SingularTestParser run the exact same AST-scan/parse-time
# logic ModelParser uses for `.py` models, so `.py` tests can use plain dbt.ref(...)/
# dbt.source(...)/dbt.config(...) calls -- identical convention to Python models, no
# special syntax. `ModelParser.parse_python_model` doesn't reference `self` for
# anything, so it's safe to call unbound against a SingularTestParser instance.
# SQL test files are unaffected (fall through to the original render_update).
#
# Consequence: `parse_python_model` also runs PythonValidationVisitor, which requires
# the file to define exactly one function literally named "model" (not "test"). Rather
# than reimplementing the AST-scanning logic under a different function-name convention,
# a trivial `def model(dbt, pl): return test(dbt, pl)` wrapper is appended to raw_code
# before parsing -- satisfies the validator (which only checks the "model" function's
# own signature), while PythonParseVisitor's ref/source/config scan still walks the
# whole file and finds calls inside the user's actual `def test(...)` regardless of
# nesting. The mutation persists onto compiled_code too, so no other code needs to know
# about this -- _run_python_model's `namespace["model"](...)` call finds the wrapper,
# which forwards to the user's `test`.
_TEST_ENTRYPOINT_WRAPPER = "\n\n\ndef model(dbt, pl):\n    return test(dbt, pl)\n"

try:
    from dbt.node_types import ModelLanguage
    from dbt.parser.models import ModelParser, verify_python_model_code
    from dbt.parser.singular_test import SingularTestParser

    _original_singular_test_render_update = SingularTestParser.render_update

    def _singular_test_render_update(
        self, node, config, validate_config_call_dict=False
    ):
        if node.language == ModelLanguage.python:
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
    _python_tests_patched = False

if not _python_tests_patched:
    from dbt.adapters.events.logging import AdapterLogger

    AdapterLogger("polars").warning(
        "Python singular tests are not supported with this version of dbt-core."
    )

Plugin = AdapterPlugin(
    adapter=PolarsAdapter,  # type: ignore[arg-type]
    credentials=PolarsCredentials,
    include_path=PACKAGE_PATH,
)
