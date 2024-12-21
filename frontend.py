import json
import asyncio

import streamlit as st
import pandas as pd
import aiohttp
import matplotlib.pyplot as plt
import seaborn as sns; sns.set_style("darkgrid")

URL_TEMPLATE = "https://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&appid={API_KEY}"

st.title("Weather analysis with streamlit")

@st.cache_data
def prepare_data(path):
    df = pd.read_csv(path)
    df = df.sort_values(by=["city", "timestamp"]).reset_index(drop=True)

    df["rolling_mean"] = None
    for city in df["city"].unique():
        df.loc[df.city == city, "rolling_mean"] = df.loc[df.city == city, "temperature"].rolling(window=30).mean()

    df_stats = df.groupby(by=["city", "season"], as_index=False).agg({"temperature": ["mean", "std"]})
    df_stats.columns = ["_".join(i) if i[1] != "" else i[0] for i in df_stats.columns]
    
    df = df.merge(df_stats, on=["city", "season"], how="left")
    
    df["anomaly"] = 0
    condition = ((df["temperature"] < df.temperature_mean - 2 * df.temperature_std) | 
                (df["temperature"] > df.temperature_mean + 2 * df.temperature_std))
    df.loc[condition, "anomaly"] = 1
    
    return df, df_stats

async def get_weather_asynch(city, API_KEY):
    url = URL_TEMPLATE.format(
        city=city,
        API_KEY=API_KEY
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
                content = await response.text()
                return json.loads(content)


def main():
    
    # upload the file
    uploaded_file = st.file_uploader("Choose CSV-file", type=["csv"])
    if uploaded_file is None:
        st.write("Please upload your CSV-file.")
        return
    
    df, df_stats = prepare_data(uploaded_file)
    
    # select city
    city = st.selectbox("Select the city: ", df["city"].unique())

    # enter the API key
    api_key = st.text_input("Enter the api key for OpenWeather", "")
    if api_key != "":
        weather = asyncio.run(get_weather_asynch(city, api_key))
        
        if "main" not in weather:
            st.error(weather["message"])
            return 
        
        curr_temp = weather["main"]["temp"]
        feel_temp = weather["main"]["feels_like"]
        st.success(f"Current temperature in {city}: **{curr_temp} °C**, feels like **{feel_temp} °C**.")
        
        normal_mean, normal_std = (df_stats
                   .loc[(df_stats.city == city) & (df_stats.season == "winter"),
                        ["temperature_mean", "temperature_std"]]
                   .values[0])
        is_anomaly_detected = "not detected"
        if (curr_temp > normal_mean + 2 * normal_std) or (curr_temp < normal_mean - 2 * normal_std):
            is_anomaly_detected = "detected"
            
        st.markdown(f"Normal temperature bounds: **({normal_mean - 2 * normal_std:.2f}°C, {normal_mean + 2 * normal_std:.2f}°C)**. Abnormal temperature is {is_anomaly_detected}.")
        
        st.markdown("#" )
        
        # descriptive statistics about the city
        st.markdown(f"##### Temperature distibution per season in {city}.")
        fig, ax = plt.subplots()
        sns.boxplot(df.loc[df.city == city], y="temperature", x="season", ax=ax)
        st.pyplot(fig)
        
        st.markdown("#" )
        
        # time-series plot of temperature with anomalies
        st.markdown(f"##### Historical temperature in {city}. Anomalies are marked with red.")
        fig, ax = plt.subplots(figsize=(15, 8))

        df_slice = df.loc[df.city == city]
        df_slice.loc[:, "timestamp"] = pd.to_datetime(df_slice["timestamp"])
        df_anomaly = df_slice.loc[df_slice.anomaly == 1]

        ax.plot(df_slice["timestamp"], df_slice["temperature"], label="Temperature")
        ax.plot(df_slice["timestamp"], df_slice["temperature_mean"] - 2 * df_slice["temperature_std"], label="Normal temperature lower bound", linestyle='dashed')
        ax.plot(df_slice["timestamp"], df_slice["temperature_mean"] + 2 * df_slice["temperature_std"], label="Normal temperature upper bound", linestyle='dashed')
        ax.scatter(df_anomaly["timestamp"], df_anomaly["temperature"], color="red", marker="*", label="anomaly")

        ax.legend()
        st.pyplot(fig)
        
        st.markdown("#" )
        
        st.markdown(f"##### Season statistics for city {city}.")
        st.dataframe(df_stats[df_stats.city == city].reset_index(drop=True))

if __name__ == "__main__":
    main()