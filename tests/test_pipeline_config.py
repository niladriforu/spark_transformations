import pipeline_config


def test_environment_defaults(spark):
    assert pipeline_config.environment() == "dev"


def test_catalog_and_schema(spark):
    assert pipeline_config.catalog() == "workspace"
    assert pipeline_config.schema() == "default"


def test_table_without_variant(spark):
    spark.conf.set("pipeline.table_variant", "")
    assert pipeline_config.table("silver_events") == "silver_events_dev"


def test_table_with_variant(spark):
    spark.conf.set("pipeline.table_variant", "propagate")
    assert pipeline_config.table("silver_events") == "silver_events_propagate_dev"
    spark.conf.set("pipeline.table_variant", "")


def test_qualified_table(spark):
    assert pipeline_config.qualified_table("raw_events") == "workspace.default.raw_events_dev"


def test_propagate_table(spark):
    assert pipeline_config.propagate_table("silver_events") == "silver_events_propagate_dev"


def test_propagate_qualified_table(spark):
    assert (
        pipeline_config.propagate_qualified_table("silver_curated_events")
        == "workspace.default.silver_curated_events_propagate_dev"
    )


def test_shared_qualified_table(spark):
    assert (
        pipeline_config.shared_qualified_table("raw_events") == "workspace.default.raw_events_dev"
    )


def test_table_variant_getter(spark):
    spark.conf.set("pipeline.table_variant", "custom")
    assert pipeline_config.table_variant() == "custom"
    spark.conf.set("pipeline.table_variant", "")
