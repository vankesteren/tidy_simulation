"""Analysis example for tidy simulation"""

import polars as pl
import plotnine as p9

simulation_grid = pl.read_parquet("processed_data/grid.parquet")
results_table = pl.read_parquet("processed_data/results.parquet")
# results_table = pl.read_parquet("processed_data/chunked_output/*.parquet").with_columns(pl.row_index("row_id"))
analysis_df = simulation_grid.join(results_table, on="row_id", how="left")

df_agg = (
    # start with the dataframe of grid parameters and results
    analysis_df
    # Remove rows with missing data (in case the simulation is not yet done)
    .drop_nulls()
    # for each row, compute the bias and whether H0 is rejected
    .with_columns(
        bias=pl.col.estimate - pl.col.effect_size, reject=pl.col.pvalue < 0.05
    )
    # then group by all the simulation factors
    .group_by(
        ["sample_size", "effect_size", "outcome", "correction"], maintain_order=True
    )
    # aggregate over iterations, with quantile interval for the bias
    .agg(
        bias=pl.col.bias.mean(),
        bias_lo=pl.col.bias.quantile(0.025),
        bias_hi=pl.col.bias.quantile(0.975),
        power=pl.col.reject.mean(),
        n=pl.len(),
    )
    # then use a normal approximation to compute the CI for the power
    .with_columns(power_se=(pl.col.power * (1 - pl.col.power) / pl.col.n).sqrt())
    .with_columns(
        power_lo=(pl.col.power - 1.96 * pl.col.power_se).clip(0.0, 1.0),
        power_hi=(pl.col.power + 1.96 * pl.col.power_se).clip(0.0, 1.0),
    )
)

# result plot for power
plt = (
    p9.ggplot(
        df_agg.filter(pl.col.effect_size.is_in([0.2, 0.5, 0.9])),
        p9.aes(
            x="sample_size",
            y="power",
            ymin="power_lo",
            ymax="power_hi",
            color="outcome",
            linetype="correction",
        ),
    )
    + p9.geom_line()
    + p9.geom_pointrange()
    + p9.facet_wrap("effect_size", labeller="label_both")
    + p9.theme_linedraw()
)

plt.save("img/power.png", width=10, height=4, dpi=300)

# result plot for bias
plt = (
    p9.ggplot(
        df_agg.filter(pl.col.effect_size == 0.5),
        p9.aes(
            x="sample_size",
            y="bias",
            ymin="bias_lo",
            ymax="bias_hi",
            color="outcome",
            linetype="correction",
        ),
    )
    + p9.geom_line()
    + p9.geom_pointrange()
    + p9.facet_grid(cols="outcome", rows="correction", labeller="label_both")
    + p9.theme_linedraw()
)

plt.save("img/bias.png", width=10, height=7, dpi=300)
