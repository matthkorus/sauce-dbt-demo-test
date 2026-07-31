def model(dbt, session):
    dbt.config(materialized="table")

    return session.create_dataframe(
        [(1, "alpha"), (2, "beta"), (3, "gamma")],
        schema=["id", "label"],
    )
