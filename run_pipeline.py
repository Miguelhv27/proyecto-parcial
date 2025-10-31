from src.orchestrator import EcommerceDataPipeline
import sys

if __name__ == "__main__":
    config_path = "config/pipeline_config.yaml"
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    pipeline = EcommerceDataPipeline(config_path)
    pipeline.run_pipeline()
