import marimo

__generated_with = "0.13.15"
app = marimo.App(width="full", app_title="AFL Home Ground Advantage Analysis")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb
    import requests
    import json
    from io import StringIO
    return duckdb, json, mo, requests


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
def _(con, games_url, header, json, requests):
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
    return (games,)


@app.cell
def _(con, games, mo, teams):
    games_df = mo.sql(
        f"""
        select h.abbrev as h_abbrev, 
                 a.abbrev as a_abbrev,
                 g.hscore as h_score,
                 g.ascore as a_score,
                 g.year as year,
                 g.round as round,
                 h.logo
                 (g.hscore - g.ascore) as margin
        from games g 
        join teams h on g.hteamid = h.id
        join teams a on g.ateamid = a.id
        """,
        engine=con
    )
    return


if __name__ == "__main__":
    app.run()
