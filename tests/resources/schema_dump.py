from shotgun_api3 import Shotgun
import os
import pickle
import sys



def schema_dump(folder, project_entity=None):
    """
    Dump Shotgun schemas to pickle file.

    :param folder: The folder to pickle the schemas to.
    :param project_entity: The SG project entity to dump the schema for.
    """
    # Connect to Shotgun
    sg_url = os.getenv("SG_URL")
    sg_script_name = os.getenv("SG_SCRIPT_NAME")
    sg_api_key = os.getenv("SG_API_KEY")
    if not sg_url or not sg_script_name or not sg_api_key:
        raise Exception("Please set SG_URL, SG_SCRIPT_NAME and SG_API_KEY environment variables.")
    sg = Shotgun(sg_url, script_name=sg_script_name, api_key=sg_api_key)

    # Dump all schemas
    schema = sg.schema_read(project_entity=project_entity)
    schema_entity = sg.schema_entity_read(project_entity=project_entity)

    # Save to pickle file
    folder = os.path.abspath(folder)
    if not os.path.exists(folder):
        os.makedirs(folder)
    pickle_file = os.path.join(folder, "schema.pickle")
    with open(pickle_file, "wb") as f:
        pickle.dump(schema, f)
    entity_pickle_file = os.path.join(folder, "schema_entity.pickle")
    with open(entity_pickle_file, "wb") as f:
        pickle.dump(schema_entity, f)


if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: python schema_dump.py <folder>")
    if len(sys.argv) == 1:
        folder = "."
    else:
        folder = sys.argv[1]
    schema_dump(folder)
