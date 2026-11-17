"""``dg scaffold`` support: writes a starter ``defs.yaml`` for ``FMEComponent``."""

from __future__ import annotations

import dagster as dg

STARTER_ATTRIBUTES = """\
attributes:
  # Key of the FMEResource the sensor uses. Leave host and api_key commented out to use
  # a resource defined elsewhere under this key.
  resource_key: fme

  # Or uncomment both to have this component define the resource itself. Use the
  # env() function for the API key; it fails loudly if the variable is unset.
  # host: https://fme.example.com
  # api_key: "{{ env('FME_API_KEY') }}"

  build_sensor: true
  sensor_name: fme_sensor
  sensor_check_minutes: 30

  fme_asset_specs:
    - workspace: example_workspace.fmw
      key_prefix: [fme]
      include_job_asset: true
      asset_list: [example_table]
"""


class FMEComponentScaffolder(dg.Scaffolder):
    """Scaffold an ``FMEComponent`` with one example workspace and the sensor settings."""

    def scaffold(self, request: dg.ScaffoldRequest) -> None:
        """Write ``defs.yaml`` (or append to it), or a ``component.py`` stub for Python."""
        if request.scaffold_format == "python":
            from dagster.components.component_scaffolding import scaffold_component

            scaffold_component(request)
            return

        document = f"type: {request.type_name}\n\n{STARTER_ATTRIBUTES}"
        # `append` (`dg scaffold defs --append`) was added to ScaffoldRequest after 1.11.
        if getattr(request, "append", False):
            with request.target_path.open("a", encoding="utf-8") as f:
                f.write(f"---\n{document}")
        else:
            (request.target_path / "defs.yaml").write_text(document, encoding="utf-8")
