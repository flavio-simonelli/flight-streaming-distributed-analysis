import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Set the path to your source Excel file
file_path = "stats.xlsx"

# Define explicit column names mapping values for all metrics
# The internal standard deviation columns are labeled but ignored for error bar calculation
col_names = [
    'Para',
    'Trans_Thr_In_Val', 'Trans_Thr_In_Internal_Std',
    'Trans_Thr_Out_Val', 'Trans_Thr_Out_Internal_Std',
    'Trans_Q1_Val', 'Trans_Q1_Internal_Std',
    'Trans_Q2_Val', 'Trans_Q2_Internal_Std',
    'Trans_Q3_Val', 'Trans_Q3_Internal_Std',
    'Steady_Thr_In_Val', 'Steady_Thr_In_Internal_Std',
    'Steady_Thr_Out_Val', 'Steady_Thr_Out_Internal_Std',
    'Steady_Q1_Val', 'Steady_Q1_Internal_Std',
    'Steady_Q2_Val', 'Steady_Q2_Internal_Std',
    'Steady_Q3_Val', 'Steady_Q3_Internal_Std'
]

# Load the Excel file skipping the first two complex header rows
df = pd.read_excel(file_path, skiprows=2, header=None, names=col_names)

# Drop spacing/empty rows and clean whitespace from the parameter column
df = df.dropna(subset=['Para'])
df['Para'] = df['Para'].astype(str).str.strip()

# Extract the base system category letter and the parallelization level number
df['Scenario'] = df['Para'].str[0]
df['Parallelizzazione'] = df['Para'].str[1:]

# Group categorical definitions for axis rendering and plotting order
scenari = ['S', 'M', 'F']
livelli_parallelizzazione = ['1', '3', '6']

# List of all experimental metrics to iterate through and chart
metriche = ['Thr_In', 'Thr_Out', 'Q1', 'Q2', 'Q3']

# Iterate through each metric to aggregate data and export charts
for metrica in metriche:

    # Calculate both the mean and the standard deviation directly from the run values (_Val)
    # This completely ignores the internal standard deviation columns from the Excel sheet
    df_grouped = df.groupby(['Scenario', 'Parallelizzazione']).agg({
        f'Trans_{metrica}_Val': ['mean', 'std'],
        f'Steady_{metrica}_Val': ['mean', 'std']
    }).reset_index()

    # Flatten the multi-index columns resulting from the double aggregation
    df_grouped.columns = [
        'Scenario', 'Parallelizzazione',
        f'Trans_{metrica}_Val', f'Trans_{metrica}_Std',
        f'Steady_{metrica}_Val', f'Steady_{metrica}_Std'
    ]

    # Handle cases with only a single run by converting NaN standard deviations to 0
    df_grouped[f'Trans_{metrica}_Std'] = df_grouped[f'Trans_{metrica}_Std'].fillna(0)
    df_grouped[f'Steady_{metrica}_Std'] = df_grouped[f'Steady_{metrica}_Std'].fillna(0)

    # Assign target output directory based on the structural nature of the metric
    if 'Thr' in metrica:
        output_dir = 'throughput'
    else:
        output_dir = 'query'

    # Ensure the target directory structure exists before saving
    os.makedirs(output_dir, exist_ok=True)

    # Generate independent charts for both transient and steady state phases
    for fase in ['Trans', 'Steady']:
        fig, ax = plt.subplots(figsize=(11, 6))

        x = np.arange(len(scenari))
        width = 0.25
        colori = {'1': '#7fb3d5', '3': '#2471a3', '6': '#154360'}

        # Plot bars side-by-side for each parallelization factor
        for i, p in enumerate(livelli_parallelizzazione):
            valori = []
            errori = []

            for l in scenari:
                riga = df_grouped[(df_grouped['Scenario'] == l) & (df_grouped['Parallelizzazione'] == p)]
                if not riga.empty:
                    valori.append(riga[f'{fase}_{metrica}_Val'].values[0])
                    errori.append(riga[f'{fase}_{metrica}_Std'].values[0])
                else:
                    valori.append(0)
                    errori.append(0)

            posizione_barre = x + (i - 1) * width
            ax.bar(
                posizione_barre, valori, width, yerr=errori, capsize=6,
                label=f'Parallelization {p}', color=colori[p], edgecolor='black', alpha=0.9
            )

        # Apply labels, titles, and formatting properties to the chart axes
        fase_label = 'TRANSIENT' if fase == 'Trans' else 'STEADY STATE'
        ax.set_title(f'{metrica} Comparison - {fase_label}', fontsize=14, fontweight='bold', pad=15)
        ax.set_xlabel('Scenario', fontsize=12, labelpad=10)
        ax.set_ylabel(f'Mean Value [{metrica}]', fontsize=12)

        ax.set_xticks(x)
        ax.set_xticklabels(scenari, fontsize=12, fontweight='bold')
        ax.legend(title='Parallelization Level', loc='upper left')
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        plt.tight_layout()

        # Build file name and save the figure directly to disk without displaying UI
        filename = f"{metrica.lower()}_{fase.lower()}_comparison.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300)
        plt.close(fig)