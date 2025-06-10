import marimo

__generated_with = "0.13.15"
app = marimo.App(
    width="full",
    app_title="AFL Home Ground Advantage Analysis",
    sql_output="polars",
)


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb
    import requests
    import json
    from io import StringIO
    import plotly.express as px
    return duckdb, json, mo, pl, px, requests


@app.cell
def _(duckdb):
    con = duckdb.connect("results.db")

    # Load data based on the selected data source
    header = {
        "User-Agent": "Mr Matheson's Data Analytics Work - geoff.matheson@education.vic.gov.au"
    }
    return con, header


@app.cell
def db_load_teams(con, header, json, requests):
    games_url = "https://api.squiggle.com.au/?q=games;year="
    teams_url = "https://api.squiggle.com.au/?q=teams"

    # Load the Teams data

    with open("teams.json", "w") as _f:
        t_data = requests.get(teams_url, headers=header)
        teams_dict = json.loads(t_data.content)
        _f.write(json.dumps(teams_dict["teams"]))

    con.execute(
        """CREATE OR REPLACE TABLE TEAMS AS (
              SELECT id, abbrev, name, logo, debut  FROM read_json('teams.json', format = 'auto'))"""
    )

    con.execute("""
        ALTER TABLE TEAMS ADD PRIMARY KEY (id);""")
    return (games_url,)


@app.cell
def _(games_url, header, json, requests):
    games = []
    with open("games.json", "w") as _f:
        for year in range(2010, 2025):
            data = requests.get(games_url + str(year), headers=header)
            games.append(json.loads(data.content)["games"])
        all_games = []

        for g in games:
            for x in g:
                all_games.append(x)
        _f.write(json.dumps(all_games))

    return (games,)


@app.cell
def _(con):

    con.execute(
        """
        CREATE OR REPLACE TABLE GAMES AS (
        SELECT * FROM read_json('games.json', format='array',
        columns = {id: 'INTEGER',
        year: 'INTEGER',
        venue: 'VARCHAR',
        round: 'INTEGER',
        hteamid: 'VARCHAR',
        ateamid: 'VARCHAR',
        hgoals: 'INTEGER',
        hbehinds: 'INTEGER',
        hscore: 'INTEGER',
        agoals: 'INTEGER',
        abehinds: 'INTEGER',
        ascore: 'INTEGER',
        is_grand_final: 'INTEGER',
        }
        ))
        """
    )

    # Update Games table to give just one name for "Kardinia Park" and "GMHBA Stadium"

    con.execute("UPDATE GAMES SET venue = 'Kardinia Park' WHERE venue = 'GMHBA Stadium'; CHECKPOINT;")

    con.execute('''
        DELETE FROM GAMES WHERE year = 2020;
    ''')
    return


@app.cell
def _(con, games, mo, teams):
    games_df = mo.sql(
        f"""
        select
            h.abbrev as h_abbrev,
            h.name as h_name,
            a.abbrev as a_abbrev,
            a.name as a_name,
            g.hscore as h_score,
            g.ascore as a_score,
            g.round as round,
            venue,
            (g.hscore - g.ascore) as h_margin,
            case
                when (g.hscore - g.ascore) > 0 then 'Home Win'
                when (g.hscore - g.ascore) < 0 then 'Away Win'
                else 'Draw'
            end as result
        from
            games g
            join teams h on g.hteamid = h.id
            join teams a on g.ateamid = a.id
        """,
        engine=con
    )
    return (games_df,)


@app.cell
def _(games_df, px):
    px.density_heatmap(games_df, x="h_score", y="a_score",title="Scores Heatmap", marginal_x="box", marginal_y="box")
    return


@app.cell
def _(con, games_df, pl):
    venue_count = con.execute("SELECT venue, count(*) matches from games GROUP BY venue ORDER BY matches DESC;").pl()
    common_venues = venue_count.filter(pl.col("matches") > 50).select("venue").to_series().to_list()
    common_venues
    venue_games = games_df.filter(pl.col("venue").is_in(common_venues))

    return (venue_games,)


@app.cell
def _(px, venue_games):
    px.histogram(venue_games, x="venue",  color="result", title="Game Results across Venues (min 50 games)",  barmode="stack", barnorm="percent")
    return


@app.cell
def _(games_df, pl, px):
    # Diving Deeper into Geelong
    geel_games = games_df.filter((pl.col("h_abbrev") == "GEE") | (pl.col("a_abbrev") == "GEE"))

    # Add column for whether Geelong is home or away
    geel_games = geel_games.with_columns(
        pl.when(pl.col("h_abbrev") == "GEE").then(pl.lit(True)).otherwise(pl.lit(False)).alias("Home")
    )

    # Add column for whether Geelong won
    geel_games = geel_games.with_columns(
        pl.when(pl.col("h_abbrev") == "GEE").then(pl.col("h_margin") > 0).otherwise(pl.col("h_margin") < 0).alias("Geelong Win")
    )

    px.histogram(data_frame=geel_games, x="round", color="Geelong Win", barmode="group")
    return


if __name__ == "__main__":
    app.run()
