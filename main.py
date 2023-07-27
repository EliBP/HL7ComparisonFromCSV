import pandas as pd
import hl7
import re

# Set your file names
output_file = 'output_messages_2.csv'
example_file = 'example_messages.csv'
# output_file = 'example_messages_copy.csv'

# Define HL7 segment names to process
segment_names = ['MSH', 'EVN', 'PID', 'PD1', 'ROL', 'NK1', 'PV1', 'ROL', 'DB1']

# Define fields to ignore in the comparison
ignore_fields = {
    'MSH': [7, 10, 11],
    'EVN': [2, 4, 5, 6],
    'PID': [3, 11, 13],
    'NK1': [5],
    'PV1': [6, 36, 41, 45]
}

#~~~Things that will never be correct~~~
#MSH7_Date/time_Of_Message: uses current time
#MSH10_Message_control_ID: randomly generated
#MSH11_Processing_ID: testing flag 'T' instead of 'P'
#EVN2_Recorded_Date: using current date
#EVN4_Event/Reason_Code: hardcoded in
#EVN5_Operator_ID: hardcoded in
#EVN6_Event_Occured: using current date for now

#~~~Unimportant problems I'll correct later~~~
#PID3_Patient_ID: Only sending 1 for now
#PID11_Patient_Address: KNOWN ISSUE WITH COUNTY CODE
#NK1_5_Phone_Number: KNOWN ISSUE WITH PHONE TYPE LABELS
#PID_13_Phone_Number: KNOWN ISSUE WITH PHONE TYPE LABELS

#~~~Things that quickly get updated~~~
#PV1_6_Last_Location: example is often out of date
#PV1_36_Discharge_Disposition: example is often out of date
#PV1_41_Patient_Status: example is often out of date
#PV1_45_Discharge_Time: example is often out of date


# Function to add newline characters before segment names for proper HL7 parsing
def parse_message(message):
  for name in segment_names:
    message = re.sub(f'(?<=[^\r])({name}\|)', r'\r\1', message)
  return hl7.parse(message)


# Function to extract HL7 segments from parsed messages
def extract_segments(msg):
  # Initialize an empty dictionary to store segments
  segments = {name: [] for name in segment_names}

  # Add each segment to the appropriate list in the dictionary
  for segment in msg:
    name = str(segment[0][0])
    if name in segments:
      segments[name].append(segment)
  return segments


# Function to load and process CSV data
def process_csv(file_name):
  df = pd.read_csv(file_name, header=None, names=['message'])
  df['parsed'] = df['message'].apply(parse_message)  # Parse HL7 messages
  df['segments'] = df['parsed'].apply(
      extract_segments)  # Extract the relevant HL7 segments
  # Extract Patient ID for merging, handling cases where PID or field doesn't exist
  df['PatientID'] = df['segments'].apply(
      lambda segments: str(segments.get('PID', [[[]]])[0][3][0][0])
      if len(segments.get('PID', [[[]]])[0]) > 3 else None)

  return df


# Load and process CSV data
output_df = process_csv(output_file)
example_df = process_csv(example_file)

# Merge the two dataframes based on PatientID
merged_df = pd.merge(output_df,
                     example_df,
                     on='PatientID',
                     suffixes=('_output', '_example'))


# Function to compare HL7 segments and print differences
def compare_segments(row):
  for segment_name in segment_names:
    output_segments = row['segments_output'].get(segment_name, [])
    example_segments = row['segments_example'].get(segment_name, [])

    # Iterate over each pair of corresponding segments, with 'enumerate' to track the segment number
    for segment_num, (output_segment, example_segment) in enumerate(zip(
        output_segments, example_segments),
                                                                    start=1):
      # Skip if segment is not present in either output or example
      if not output_segment or not example_segment:
        continue

      # Iterate over each field in the segment, skipping ignored fields
      for i in range(min(len(output_segment), len(example_segment))):
        if i in ignore_fields.get(segment_name, []):
          continue  # Skip ignored fields

        # If field values do not match, print a difference message
        if output_segment[i] != example_segment[i]:
          print(
              f'----\nPatient ID {row["PatientID"]}: {segment_name} segment {segment_num} Field {i} does not match.\nOutput:\n{output_segment[i]}\nExample:\n{example_segment[i]}\n'
          )


# Compare segments for each row in the merged dataframe
merged_df.apply(compare_segments, axis=1)

print("\n\n\n DONE SEARCHING FOR MISMATCHES")
