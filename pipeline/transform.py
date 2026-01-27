"""Preliminary Transform Script for the pipeline"""
from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


def parse_scientific_name(value: object) -> Optional[str]:
    """Parses scientific name if there is one"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    string = str(value).strip()
    if not string or string.lower() in {"nan", "none", "null"}:
        return None

    if string.startswith("[") and string.endswith("]"):
        try:
            parsed_string = ast.literal_eval(string)
            if isinstance(parsed_string, list) and parsed_string:
                return str(parsed_string[0]).strip() or None
        except (ValueError, SyntaxError):
            pass

    return string


PHONE_EXT_RE = re.compile(r"(?:ext\.?|x)\s*(\d+)\s*$", re.IGNORECASE)


def normalise_phone(value: object) -> Tuple[Optional[str], Optional[str]]:
    """Normalises phone numbers in different formats"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None

    string = str(value).strip()
    if not string:
        return None, None

    ext = None

    number = PHONE_EXT_RE.search(string)
    if number:
        ext = number.group(1)
        string = PHONE_EXT_RE.sub("", string).strip()

    string = re.sub(r"^\s*00", "+", string)
    has_plus = string.startswith("+")
    digits = re.sub(r"\D", "", string)

    if not digits:
        return None, ext

    phone = f"+{digits}" if has_plus else digits
    return phone, ext


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", type=Path, required=True)
    parser.add_argument("--output", "-o", type=Path, required=True)
    args = parser.parse_args()

    df = pd.read_csv(args.input)

    df = df.copy()

    df["plant_id"] = pd.to_numeric(
        df["plant_id"], errors="raise").astype("int64")
    df["plant_name"] = df["plant_name"].astype(str).str.strip()
    df["scientific_name"] = df["scientific_name"].apply(parse_scientific_name)

    df["botanist_name"] = df["botanist_name"].astype(str).str.strip()
    df["botanist_email"] = df["botanist_email"].astype(
        str).str.strip().str.lower()

    phone_data = df["botanist_phone"].apply(normalise_phone)
    df["botanist_phone"] = phone_data.apply(lambda t: t[0])
    df["botanist_phone_ext"] = phone_data.apply(lambda t: t[1])

    df["city"] = df["city"].astype(str).str.strip()
    df["country_name"] = df["country_name"].astype(str).str.strip()

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df.loc[~df["latitude"].between(-90, 90), "latitude"] = pd.NA
    df.loc[~df["longitude"].between(-180, 180), "longitude"] = pd.NA

    df["soil_moisture"] = round(pd.to_numeric(
        df["soil_moisture"], errors="coerce"), 2)
    df.loc[df["soil_moisture"] < 0, "soil_moisture"] = pd.NA

    df["temperature"] = round(pd.to_numeric(
        df["temperature"], errors="coerce"), 2)

    df["last_watered_ts"] = pd.to_datetime(df["last_watered"], errors="coerce")
    df["recording_taken_ts"] = pd.to_datetime(
        df["recording_taken"], errors="coerce")

    df["last_watered_ts"] = df["last_watered_ts"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S")
    df["recording_taken_ts"] = df["recording_taken_ts"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S")

    df_out = df[
        [
            "plant_id",
            "plant_name",
            "scientific_name",
            "botanist_name",
            "botanist_email",
            "botanist_phone",
            "botanist_phone_ext",
            "city",
            "country_name",
            "latitude",
            "longitude",
            "last_watered_ts",
            "recording_taken_ts",
            "soil_moisture",
            "temperature",
        ]
    ]

    df_out.to_csv(args.output, index=False)
    print(f"Wrote cleaned data to {args.output}")


if __name__ == "__main__":
    main()
