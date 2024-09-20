import os

symPath='..'
directory = os.path.realpath(symPath)

existing_project_id = 'your-project-id1234'
new_project_id = 'your-project-id1234'

for root, dirs, files in os.walk(directory):
    for file in files:
        if file.endswith('.py'):
            file_path = os.path.join(root, file)
            print(f"Replacing text in file: {file_path}")
            with open(file_path, 'r') as file:
                content = file.read()

            # Replace the old_text with new_text
            content = content.replace(existing_project_id, new_project_id)

            # Write the modified content back to the file
            with open(file_path, 'w') as file:
                file.write(content)




