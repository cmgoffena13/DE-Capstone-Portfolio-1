#12.5.0 old version, downgrading to Python 11 for snowpark to work
FROM quay.io/astronomer/astro-runtime:11.15.1
RUN mkdir -p /home/astro/.dbt
COPY profiles.yml /home/astro/.dbt/profiles.yml