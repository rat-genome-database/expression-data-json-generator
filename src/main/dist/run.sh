#!/bin/bash
. /etc/profile

APPNAME=expression-data-json-generator
APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

# Default values
OUTPUT_DIR=/home/rgddata/data/expression_json
THREADS=4

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -t|--threads)
            THREADS="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Create output directory if it doesn't exist
mkdir -p $OUTPUT_DIR

echo "Starting $APPNAME at $(date)"
echo "Output directory: $OUTPUT_DIR"
echo "Threads: $THREADS"

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/resources/log4j2.xml \
    -jar lib/${APPNAME}.jar \
    --output $OUTPUT_DIR \
    --threads $THREADS \
    2>&1 | tee run.log

echo "Completed at $(date)"

