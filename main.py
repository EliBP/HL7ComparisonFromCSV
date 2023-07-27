import pandas as pd
import hl7

# Replace these with your actual file paths
output_file = 'output_messages.csv'
example_file = 'example_messages.csv'

# Load the CSV files into pandas DataFrames
# header=None indicates that there are no headers
# names=['message'] assigns a name to the column
output_df = pd.read_csv(output_file, header=None, names=['message'])
example_df = pd.read_csv(example_file, header=None, names=['message'])

# Parse the HL7 messages and extract the PID segment
output_df['parsed'] = output_df['message'].apply(hl7.parse)
example_df['parsed'] = example_df['message'].apply(hl7.parse)

output_df['PID'] = output_df['parsed'].apply(lambda msg: msg.segment('PID'))
example_df['PID'] = example_df['parsed'].apply(lambda msg: msg.segment('PID'))

# Extract the Patient ID field (PID.3) and use it to match the messages
output_df['PatientID'] = output_df['PID'].apply(lambda pid: pid[3])
example_df['PatientID'] = example_df['PID'].apply(lambda pid: pid[3])

# Merge the two DataFrames on PatientID
merged_df = pd.merge(output_df,
                     example_df,
                     on='PatientID',
                     suffixes=('_output', '_example'))

# Now you can compare all fields between the matched messages
for _, row in merged_df.iterrows():
  # Get the output and example PID segments
  output_pid = row['PID_output']
  example_pid = row['PID_example']

  # Get the length of the shorter PID segment
  min_length = min(len(output_pid), len(example_pid))

  # Iterate over each field in the example PID segment up to min_length
  for i in range(min_length):
    # If the field is not the same in the output and example segments, print a message
    if output_pid[i] != example_pid[i]:
      print(
          f'Patient ID {row["PatientID"]}: Field {i} does not match. Output: {output_pid[i]}, Example: {example_pid[i]}'
      )
