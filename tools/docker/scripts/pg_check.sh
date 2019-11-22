#!/usr/bin/env bash
while ! nc -z postgres 5432;
do
  echo sleeping;
  sleep 1;
done;
echo Postgres Connected!;