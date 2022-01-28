import configparser
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData

def main():
    """
    - Establishes connection with the database on Amazon Redshift
    
    - Create the ER diagram of the database
    
    - Finnaly close the connection

    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    DWH_DB= config.get("CLUSTER","DB_NAME")
    DWH_DB_USER= config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER","DB_PORT")
    DWH_ENDPOINT = config.get('CLUSTER', 'HOST')

    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)

    graph = create_schema_graph(metadata=MetaData(conn_string))
    graph.write_png('./img/ER.png')
    

if __name__ == "__main__":
    main()
