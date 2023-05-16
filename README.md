# datagen

This script combines the continent-level data files produced by datagen into single, global-level files.

The following files are combined and created:
- basin.json
- cycle_passes.json
- hivdisets.json
- metrosets.json
- passes.json
- reach_node.json
- reaches.json
- s3_list.json
- sicsets.json

Files are written out to directory referenced by 'datadir' command line argument.

# installation

Build a Docker image: `docker build -t combine .`

# execution

**Command line arguments:**
- -c: Path to continent JSON file
- -d: Path to directory that contains `datagen` output data
- -x: Indicate the `datagen` output continent-level JSON files should be deleted

**Execute a Docker container:**

```
# Docker run command
docker run --rm --name combiner -v /mnt/combiner:/data combiner:latest -c /data/continent.json -d /data/datagen_output -x
```