# atd-kits

Python scriptss for managing integrations with ATD's KITS traffic management system.

## Scripts

### Signal Status Publisher (`signal_status_publisher.py`)

Fetches traffic signal statuses from traffic management system (KITS) and publishes to Open Data Portal.


## Docker CI

The docker image `attdocker/atd-kits` will be rebuilt and pushed to Docker hub on any push to a github branch. If `production` is the target branch, the image will be tagged with `:production` otherwise it will be tagged with `staging`.

Note that ymmv running the Docker-hub-hosted images on Apple silicon. You may need to manually build from the `Dockerfile` provided.
