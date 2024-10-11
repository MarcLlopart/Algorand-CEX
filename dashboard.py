import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# Example dataframe setup (replace this with your actual data)
df = pd.read_csv('data/CEX_balances.csv')  # Load your actual data
df['Date'] = pd.to_datetime(df['Date'])
balance_columns = [col for col in df.columns if col.endswith(' Balance')]

# Sidebar: Create a multi-select widget to allow user selection
selected_columns = st.sidebar.multiselect(
    "Select the balances to plot:",
    options=balance_columns + ['Total'],  # Include 'Total' as an option
    default=['Total']  # Default to 'Total'
)

# Plot the selected columns
st.write("### Daily Algo Balance Plot")

# Create the plot using matplotlib
fig, ax = plt.subplots(figsize=(10, 6))

# Ensure 'Total' is only plotted if selected
for column in selected_columns:
    ax.plot(df['Date'], df[column], label=column)

# Customize the plot
ax.set_title('Selected Algo Balances Over Time')
ax.set_xlabel('Date')
ax.set_ylabel('Balance')

# Set x-ticks to display dates without duplicates
ax.set_xticks(df['Date'][::len(df)//10])  # Show every nth date to avoid clutter
ax.set_xticklabels(df['Date'].dt.strftime('%Y-%m-%d')[::len(df)//10], rotation=45, ha='right')

# Format the y-axis to show large numbers as 'B', 'M', etc.
ax.yaxis.set_major_formatter(
    mtick.FuncFormatter(lambda x, _: f'{x*1e-9:.1f}B' if x >= 1e9 else f'{x*1e-6:.1f}M' if x >= 1e6 else f'{x:.0f}')
)

# Add legend, only show when there are multiple lines
if len(selected_columns) > 0:
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1), title='Entities')

# Add gridlines
ax.grid(True)

# Ensure layout fits
plt.tight_layout()

# Display the plot in Streamlit
st.pyplot(fig)