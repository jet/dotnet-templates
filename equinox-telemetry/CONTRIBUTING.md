There are 2 core themes in this telemetry template:

* streamlining the experience of developers simply wanting to get telemetry out of the box
* facilitating developers enhancing the dashboards by establishing a convention

One would find that much of the quirks that drive the developing experience for this template is Grafana's limitations.
In an ideal world, the most important elements: the panels and / or panel groups should be easily mixed and matched in
some intuitive fashion based on a few high-level decisions:

* usage of Equinox and / or Propulsion
* choice of data store: some subset of Cosmos, EventStore, Kafka, etc.
* additional features that were opted in: `Propulsion.Feed`

The goal is to create our own means to execute this mixing and matching. And the `build-dashboards.py` Python script is
the culmination of the limited understanding from:

* the [official spec on the Grafana website](https://grafana.com/docs/grafana/v8.3/dashboards/json-model/)
* observations in how changing the JSON affects the rendering

## Flow

Below is the expected flow for a developer wishing to enhance the dashboards

* using Grafana's facility for provisioning a dashboard based on files in directories
* load dashboard in UI and make changes in the normal, interactive way (i.e.: not directly against the JSON)
* copy and paste JSON model (i.e.: spec) into version control

Further

* spec files would split by base Equinox vs base Propulsion dashboards, then data stores
* any new grouping should lead to extension of the `build-dashboards.py` command line arg parser

> Currently the split is just by Equinox vs. Propulsion.
>
> In addition, Python was chosen because it is generally hard to figure what programming runtime comes pre-installed in
> everyone's machines or where it would require the least effort to get developers wanting to just have telemetry
> working in an Equinox app set up. Mac and the majority of Linux users reasonably have this pre-installed. Windows
> users would usually solve this with standard, straightforward installers.
>
> Python _3_ was chosen because overall development of the language is sunsetting version 2.

Much of the Python code is visually sectioned with comments and best effort has been made to make every step in
generating an output JSON understandable.

Below, the main fields driving the JSON that are to be discussed will be around

* panels
* variables

## Core Aspects Of Quirks

> Brief disclaimer: much of what's articulated is clearly not from someone that's a contributor to the Grafana source
> code. It'd be swell if the grasping for straws below was updated after being better armed with knowledge from the
> source code and seeing how JSON specs _really_ translate to what is rendered on the screen.

### Panel Positioning

Let's start with the `panels` key at the root. From what I observed in Grafana 7.X and 8.X, at least, the main 2
elements for one's mental model are panels and panel groups.

There's a coordinate system based on a grid that is 24 units horizontally. In the vertical direction, the grid extends
to infinity.

When scripting the programmatic placement of panels onto that grid, one must visualize this 2-dimensional grid with the
following attributes with distance measured in what we'll just refer to as "units":

* `w` width (value goes up to 24 units, the grid's maximum horizontal length)
* `h` height
* `x` x-coordinate on the grid
* `y` y-coordinate on the grid

Panel groups are simple in that they're expected to take the whole width of the screen (i.e.: `w` equals 24) and always
have a height of 1 unit. However, because they can either be collapsed or not (i.e.: JSON key of `collapsed` being
`true` or `false`), only if they're collaped would the member panels sit within a child `panels` key. Otherwise, the
member panels would exist as sibling JSON to the panel group.

Presently, not much effort has been spent to implement "tight stacking", as it were, for both the horizontal and
vertical directions. Within reason, it is expected that the JSON that's commited to version control would've had a
sensible rendering prior to this.

## Dashboard Variables

These sit, starting at the root, at `templating.list`. Each child JSON of this array corresponds to a dashboard variable
that visually manifests as a dropdown at the top of the dashboard. To date, the only relevance is to conditionally
render a `group` variable depending on whether Propulsion panels are to show up (i.e.: Equinox doesn't have a notion of
groups).

## Shared Quirks

Both panels and variables JSONs have a `datasource` key sitting somewhere within them. Notably, in version 7.X of
Grafana, this would simply be a strong. But 8.X has it as an object. This is currently the cause of friction for
updating panels while on two different versions of Grafana for development.

# Appendix

* Grafana's provisioning utility: https://grafana.com/docs/grafana/latest/administration/provisioning/
