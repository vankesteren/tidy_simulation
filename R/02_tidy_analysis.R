library(tidyverse)
library(nanoparquet)

# load the grid
simulation_grid <- read_parquet("processed_data/grid.parquet")

# load the results
# if using chunked:
# results_list <- lapply(Sys.glob("output/*.parquet"), read_parquet)
# results_table <- bind_rows(results_list)
# otherwise:
results_table <- read_parquet("processed_data/results.parquet")


# combine them
# if results are complete:
# analysis_df <- bind_cols(simulation_grid, results_table)

# if results are not yet complete:
analysis_df <- left_join(
  x = simulation_grid |> rowid_to_column(), 
  y = results_table |> rowid_to_column(), 
  by = join_by(rowid)
)

df_agg <- 
  # start with the dataframe of grid parameters and results
  analysis_df |> 
  # Remove rows with missing data (in case the simulation is not yet done)
  drop_na() |> 
  # for each row, compute the bias and whether H0 is rejected
  mutate(
    difference = estimate - effect_size,
    reject = pvalue < 0.05
  ) |> 
  # then group by all the simulation factors
  group_by(sample_size, effect_size, outcome, correction) |> 
  # aggregate over iterations, with quantile interval for the bias
  summarize(
    bias = mean(difference),
    bias_lo = quantile(difference, probs = 0.025),
    bias_hi = quantile(difference, probs = 0.975),
    power = mean(reject),
    n = n(), 
    .groups = "drop"
  ) |> 
  # then use a normal approximation to compute the CI for the power
  mutate(
    power_se = sqrt(power * (1 - power) / n),
    power_lo = pmax(0, power - 1.96 * power_se),
    power_hi = pmin(1, power + 1.96 * power_se)
  )

# result plot for power
df_agg |>
  filter(effect_size %in% c(0.2, 0.5, 0.9)) |>
  ggplot(
    aes(
      x = sample_size,
      y = power,
      ymin = power_lo,
      ymax = power_hi,
      colour = outcome,
      linetype = correction
    )
  ) +
  geom_line() +
  geom_pointrange() +
  facet_wrap(vars(effect_size), labeller = "label_both") +
  theme_linedraw()

ggsave("img/power.png", width = 10, height = 4, dpi = 300)

# result plot for bias
df_agg |>
  filter(effect_size == 0.5) |>
  ggplot(
    aes(
      x = sample_size,
      y = bias,
      ymin = bias_lo,
      ymax = bias_hi,
      colour = outcome,
      linetype = correction
    )
  ) +
  geom_line() +
  geom_pointrange() +
  facet_grid(rows = vars(correction), cols = vars(outcome), labeller = "label_both") +
  theme_linedraw()

ggsave("img/bias.png", width = 10, height = 7, dpi = 300)