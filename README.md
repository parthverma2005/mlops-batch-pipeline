# MLOps Batch Job

## Run locally
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

## Run with Docker
docker build -t mlops-task .
docker run --rm mlops-task

## Output
- metrics.json
- run.log

## Example metrics.json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4990,
  "latency_ms": 120,
  "seed": 42,
  "status": "success"
}
