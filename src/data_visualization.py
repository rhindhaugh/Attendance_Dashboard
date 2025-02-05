import plotly.express as px

def plot_arrival_distribution(df: pd.DataFrame):
    fig = px.histogram(df, x='hour', nbins=24, title='Distribution of Arrival Times')
    fig.show()  # or return fig if used in Streamlit / Dash
