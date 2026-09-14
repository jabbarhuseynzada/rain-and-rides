{#
    Label an hour by how wet it was. One definition, used everywhere, so a chart
    and a test can never disagree about what "heavy rain" means.

    Thresholds: light rain up to 2.5 mm in an hour, heavy above that.
    Snow wins over rain, because precipitation_mm already includes melted snow.
#}
{% macro rain_label(precipitation_mm, snowfall_cm) %}
    case
        when {{ snowfall_cm }} > 0            then 'snow'
        when {{ precipitation_mm }} is null   then 'unknown'
        when {{ precipitation_mm }} = 0       then 'dry'
        when {{ precipitation_mm }} <= 2.5    then 'light rain'
        else                                       'heavy rain'
    end
{% endmacro %}