# Equinox Telemetry

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new eqxTelemetry

The purpose of this template is to allow a developer to quickly get set up with dashboards while running their Equinox /
Propulsion application(s) locally. This setup uses Docker to allow one to reproducibly spin up sandboxed instances of
Prometheus and Grafana that come pre-wired and pre-provisioned with the canonical Equinox dashboards.

Of note: the versions of the dependencies are locked down to:

* Prometheus 2.26.0
* Grafana 7.5.10

These can be easily changed in the corresponding `Dockerfile`s under the `prometheus` and `grafana` directories.

Additionally, this set-up uses the very simple, Prometheus file-based service discovery mechanism to tell it what
processes to scrape for metrics. Of note:

* the file `config/targets.yml` specifies what are the target host(s) (in this case, `host.docker.internal` acting as
  `localhost` in Docker world) and port(s) to be scraped
* this file has been set up with a Docker bind-mount so that the containerized Prometheus can still see it on the host
  filesystem and any changes made to it
* Prometheus uses a file watcher in this approach, so it'll know if new application instances are to be kept track of

## Usage Instructions

The main point of change necessary is to add a line in `config/targets.yml` for the known port of a running instance of
an Equinox application. Usually, this would be spec-ed via `-p` in the other templates.

Then, run the `build-dashboards.py` Python 3 script (while in the same directory) and specify one or a combo of the
following to have the dashboard contain the panel groups relevant for your app:

* `--eqx`: to include Equinox panel groups
* `--prp`: to include Propulsion panel groups

> A help message will be displayed if you execute the script with `--help`.

This will generate the final Grafana spec and place it in the correct directory for Docker to build with.

Finally, while in the same directory as `docker-compose.yml`, simply run the following to start up Prometheus and
Grafana

```
docker compose up -d

OR

docker-compose up -d # for older versions of Docker Compose
```

and connect to `http://localhost:3000` via the browser to access Grafana.

> You may need to run `docker build` first if this is not the first time you ran the above command since the directory
> contents may have changed (i.e.: you may have initially run the Python script with different options).

After setting the admin password for the first time, the canonical Equinox dashboard can be found under the `equinox`
directory.

## Side Notes

This simple set-up is not meant for production since the Prometheus store would require a persistent volume. More
advanced users of Docker can use this template as a starting point and similarly pre-provision their instances with the
Equinox dashboard spec.

Additionally, `targets.yml` is put in a directory as opposed to being directly bind-mounted to the Prometheus container
because this approach is not supported on Windows. In fact, it is discouraged even on Linux systems according to
[this](https://github.com/moby/moby/issues/30555).

This `README.md` is meant for developers trying out Equinox or wanting to get the latest specs for the dashboards. Refer
to the `CONTRIBUTING.md` if you wish to enhance them and others' telemetry experience.
