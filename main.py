import os

import config
from get_mbid_country import check_dependencies, build_countries_csv


def main():
    run_dir = os.getcwd()
    config.set_base_dir(run_dir)

    csv_path = config.resolve_path(config.OUTPUT_CSV)
    map_output_path = config.resolve_path("artists_map_dashboard_dark.html")

    if os.path.exists(csv_path):
        print(f"Found existing CSV at {csv_path}. Building map only...")
        from get_map import build_map

        build_map(csv_path, map_output_path)
        return

    if not check_dependencies():
        raise SystemExit(1)

    df = build_countries_csv(config.PLAYLIST_URL, csv_path)
    if df is None:
        return

    from get_map import build_map

    build_map(csv_path, map_output_path)


if __name__ == "__main__":
    main()
