{% for week in weeks -%}
CALL nascar.load_agg_week_prev_track_manufacturers_results({{ series_id }}, {{ week.week_id }});
{% if not loop.last -%}
---
{% endif -%}
{% endfor -%}
