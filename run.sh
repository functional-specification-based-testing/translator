#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo -e "usage:\t$0 <mmtp_output_file> <actual_output_dir>"
    exit 2
fi
FEED="$1"
DIR="$2"

read -p "Have you generated $FEED? [(y)/n] " i
if [[ $i == "n" ]]; then
    echo "Please generate "$FEED" and retry."
    exit
fi

echo ""
echo -e "\e[1m\e[33mPopulating ./raw...\e[39m\e[0m"
rm -rf raw
mkdir -p raw
./splitter.py "$FEED" raw

echo ""
echo -e "\e[1m\e[33mPopulating ./feed & ./oracle...\e[39m\e[0m"
rm -rf feed
mkdir -p feed
rm -rf oracle
mkdir -p oracle
./convert.sh

echo ""
echo "Please run Java program to populate $DIR."
read -p "Continue? [(y)/n] " i
if [[ $i == "n" ]]; then
    exit
fi

echo ""
echo -e "\e[1m\e[33mPopulating ./res...\e[39m\e[0m"
./preprocess_test_suite.sh "$DIR"
