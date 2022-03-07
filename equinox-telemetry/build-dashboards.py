from pathlib import Path

import json
import argparse

parser = argparse.ArgumentParser(description="Specify dashboard build output.")
parser.add_argument("--eqx", action="store_true", help="Specify Equinox panel groups to be included.")
parser.add_argument("--prp", action="store_true", help="Specify Propulsion panel groups to be included.")
args = parser.parse_args()

flags = vars(parser.parse_args())
if not any(flags.values()):
    parser.error("At least one of `--eqx` or `--prp` must be specified.")

# Set baseline dashboard
with open("dashboards/base.json") as fd:
    base = json.load(fd)
    base["panels"] = []

# Step to decide set of panels to include in output
# NOTE: Current preference is for Propulsion panel groups to be on top
if args.prp:
    with open("dashboards/propulsion.json") as prp:
        prp = json.load(prp)
        base["panels"] += prp["panels"]

if args.eqx:
    with open("dashboards/equinox.json") as eqx:
        eqx = json.load(eqx)
        base["panels"] += eqx["panels"]

# Set title based on combo specified
title = "Equinox Metrics" # Default title
if args.eqx and args.prp:
    title = "Equinox/Propulsion Metrics"
elif args.eqx:
    title = "Equinox Metrics"
elif args.prp:
    title = "Propulsion Metrics"

base["title"] = title

# If Propulsion panels are not to be included, remove `group` dropdown
if not args.prp:
    ref = base["templating"]["list"]
    base["templating"]["list"] = list(filter(lambda entry: entry["name"] != "group", ref))

# If have to down-convert to 7.X style data source spec
for panel in base["panels"]:
    # NOTE: Conditional `"datasource" in panel"` is needed because it's not consistently there
    # Reference: https://github.com/grafana/grafana/issues/44506
    if "datasource" in panel and type(panel["datasource"]) is dict:
        panel["datasource"] = "$datasource"
    if panel.get("collapsed", False):
        for subpanel in panel["panels"]:
            if type(subpanel["datasource"]) is dict:
                subpanel["datasource"] = "$datasource"

# Additionally down-convert for dashboard variables
for variable in base["templating"]["list"]:
    if "datasource" in variable and type(variable["datasource"]) is dict:
        variable["datasource"] = "$datasource"

# Adjust y-coordinates / vertical positioning for included panels
# NOTE: A simplification is used here to accommodate for the fact that more than one panel can take
# up a row. A deep dive is available in `CONTRIBUTING.md`.
y_coor = 0
for index in range(len(base["panels"])):
    panel = base["panels"][index]
    panel["gridPos"]["y"] = y_coor
    if panel["gridPos"]["x"] + panel["gridPos"]["w"] == 24:
        y_coor += panel["gridPos"]["h"]

# Write out for Docker to build and include for Grafana
Path("grafana/dashboards").mkdir(exist_ok=True)
with open("grafana/dashboards/equinox.json", "w") as fd:
    json.dump(base, fd, indent=2)
