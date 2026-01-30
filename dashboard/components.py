"""Generate the components for the dashboard"""

import streamlit as st
import altair as alt


def sidebar(data):
    with st.sidebar:
        if st.button('Refresh Data'):
            st.cache_data.clear()
            st.rerun()

        plants = ['All'] + sorted(data['plant_id'].unique().tolist())
        plant_selection = st.selectbox(
            'Filter by Plant ID',
            plants,
            key='plant_filter'
        )

    return plant_selection


def soil_moisture_over_time(data):
    st.subheader('Soil Moisture')
    line1 = alt.Chart(data).mark_line(interpolate='monotone', strokeWidth=2).encode(
        x=alt.X('timestamp:T', timeUnit="hoursminutes", title="Time"),
        y=alt.Y('soil_moisture:Q', title="Soil Moisture %"),
        color=alt.Color('plant_id:N')
    )
    st.altair_chart(line1, width='stretch')


def temperature_over_time(data):
    st.subheader('Temperature')
    line2 = alt.Chart(data).mark_line().encode(
        x=alt.X('timestamp:T', timeUnit="hoursminutes", title="Time"),
        y=alt.Y('temperature:Q', title='Temperature'),
        color=alt.Color('plant_id:N')
    )
    st.altair_chart(line2, width='stretch')
