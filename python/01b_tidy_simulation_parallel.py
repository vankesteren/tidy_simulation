"""Parallel tidy simulation example for a power analysis. Run this file using `uv run 01b_tidy_simulation_parallel.py`."""

import numpy as np
import polars as pl
import statsmodels.api as sm
from polarsgrid import expand_grid
from tqdm import tqdm
from multiprocessing import Pool

# Component 1: the simulation grid
grid = expand_grid(
    sample_size=list(np.arange(4, 20)),
    effect_size=list(np.arange(0, 1.1, 0.1)),
    outcome=["post", "change"],
    correction=[False, True],
    iteration=list(range(500)),
    _categorical=True,
    _row_id=True,
)
grid = grid.with_columns(seed=np.random.randint(low=0, high=2**32 // 2, size=len(grid)))
grid.write_parquet("processed_data/grid.parquet")


# Component 2: data generation function
def generate_data(sample_size: int, effect_size: float, seed: int) -> pl.DataFrame:
    np.random.seed(seed)

    # sample pre and post variables
    treated = np.arange(sample_size) < (sample_size // 2)
    pre = np.random.normal(0, 3, sample_size)
    post = pre + np.random.normal(0, 0.3, sample_size)
    post[treated] += effect_size

    # return a tidy dataframe
    return pl.DataFrame(
        {
            "id": np.arange(sample_size),
            "treated": treated,
            "pre": pre,
            "post": post,
        }
    )


# Component 3: data analysis function
def analyze_data(
    df: pl.DataFrame, outcome: str, correction: bool
) -> tuple[float, float, bool]:
    # cast treated column to integer, needed for model fitting
    df = df.with_columns(pl.col.treated.cast(int))

    # select columns based on simulation factors
    endog = df["post"] - df["pre"] if outcome == "change" else df["post"]
    exog = df.select(["treated", "pre"]) if correction else df.select("treated")

    # create and fit the model
    mod = sm.OLS(endog.to_numpy(), sm.add_constant(exog.to_numpy()))
    res = mod.fit()

    # return values of interest, including multicollinearity indicator
    return res.params[1], res.pvalues[1], res.eigenvals[-1] < 1e-10


# Component 4: results table
# Define a function which returns one row of the results table
def run_simulation(id: int, params: dict):
    df = generate_data(
        sample_size=params["sample_size"],
        effect_size=params["effect_size"],
        seed=params["seed"],
    )
    est, pval, singular = analyze_data(
        df=df, outcome=params["outcome"], correction=params["correction"]
    )
    return (id, est, pval, singular)


# Run this function in parallel
if __name__ == "__main__":
    with Pool() as p:
        results_list = p.starmap(
            run_simulation,
            tqdm(enumerate(grid.iter_rows(named=True)), total=len(grid)),
        )
    df = pl.DataFrame(
        results_list,
        schema={"row_id": int, "estimate": float, "pvalue": float, "singular": bool},
        orient="row",
    )
    df.write_parquet("processed_data/results.parquet")
