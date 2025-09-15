library(tidyverse)
library(nanoparquet)
library(pbapply)
library(parallel)

# Component 1: the simulation grid
grid <-
  tibble(expand.grid(
    sample_size = 4:19,
    effect_size = seq(0, 1, 0.1),
    outcome     = c("post", "change"),
    correction  = c(FALSE, TRUE),
    iteration   = 1:500
  )) |>
  mutate(
    row_id = 1:n(),
    seed = as.integer(sample(2^32 / 2, n()))
  ) |>
  relocate(row_id)

write_parquet(grid, "processed_data/grid.parquet")


# Component 2: data generation function
generate_data <- function(sample_size = 16, effect_size = 0.5, seed = 45) {
  set.seed(seed)

  # sample pre and post variables
  treated <- 1:floor(sample_size / 2)
  pre <- rnorm(sample_size, sd = 3)
  post <- pre + rnorm(sample_size, mean = 1, sd = 0.3)
  post[treated] <- post[treated] + effect_size

  # return a tidy dataframe
  tibble(
    id = 1:sample_size,
    treated = id %in% treated,
    pre = pre,
    post = post
  )
}

# Component 3: data analysis function
analyze_data <- function(df, outcome = "post", correction = TRUE) {
  
  # first construct a formula
  frm <- if (outcome == "post") post ~ treated else (post - pre) ~ treated
  if (correction) frm <- update(frm, ~ . + pre)
  
  # fit the model
  fit <- lm(frm, data = df)
  
  # then, return the values we need
  return(list(
    "estimate" = unname(coef(fit)["treatedTRUE"]),
    "pvalue" = summary(fit)$coefficients["treatedTRUE", "Pr(>|t|)"],
    "converged" = fit$rank == length(coef(fit))
  ))
}

# Run the simulation to produce Component 4: the results table
# first, define a function that takes in a row idx and runs
# the simulation once
run_simulation <- function(idx) {
  args <- grid[idx, ]
  df <- generate_data(
    sample_size = args$sample_size,
    effect_size = args$effect_size,
    seed = args$seed
  )
  res <- analyze_data(
    df = df,
    outcome = args$outcome,
    correction = args$correction
  )
  res$row_id <- idx
  return(res)
}

# Then, create a function for chunking / slicing
run_chunk <- function(chunk_id, chunk_size) {
  # manually compute indices
  start_idx <- (chunk_id - 1) * chunk_size + 1
  end_idx <- min(nrow(grid), chunk_id * chunk_size)

  # compute output for single chunk
  chunk_list <- lapply(start_idx:end_idx, run_simulation)

  # create tidy data frame
  chunk_table <- bind_rows(chunk_list) |> relocate(row_id)

  # then write to output directory as parquet
  out_name <- paste0("processed_data/chunked_output/", str_pad(chunk_id, 6, pad = 0), ".parquet")
  write_parquet(chunk_table, out_name)
}

# create a parallel cluster
# on linux / mac: run the following line
# clus <- makeForkCluster(detectCores())

# on windows: run the following lines instead
clus <- makeCluster(detectCores())
clusterEvalQ(clus, {
  library(tidyverse)
  library(nanoparquet)
})
clusterExport(clus, c("grid", "generate_data", "analyze_data", "run_simulation"))

# actually compute the output
chunk_size <- 10000
num_chunks <- ceiling(nrow(grid) / chunk_size)
pbwalk(1:num_chunks, run_chunk, chunk_size = chunk_size, cl = clus)

# stop the cluster to free the resources
stopCluster(clus)
