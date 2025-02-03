import logging
import os
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from crate import client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename=f'api_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize FastAPI app
app = FastAPI(
    title="Chile Congress API",
    description="API for accessing Chile Congress data",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data Models
class ProjectBase(BaseModel):
    titulo: str
    tipo: str
    fecha: str
    numero: str
    organismo: str
    categoria: Optional[str] = None

class Project(ProjectBase):
    class Config:
        from_attributes = True

# Routes
@app.get("/")
async def root():
    return {"message": "Chile Congress API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/projects", response_model=List[Project])
async def get_projects():
    try:
        # Implement database connection
        CRATEDB_PASSWORD = os.getenv('CRATEDB_PASSWORD')
        conn = client.connect("https://kitsune-test.aks1.westeurope.azure.cratedb.net:4200", username="admin", password=CRATEDB_PASSWORD, verify_ssl_cert=True)

        with conn:
            cursor = conn.cursor()
            cursor.execute("SELECT titulo, tipo, fecha, numero, organismo, categoria FROM crate ORDER BY fecha DESC")
            results = cursor.fetchall()

            # Convert row objects to dictionaries
            projects = []
            for row in results:
                project_dict = {
                    'titulo': row[0],
                    'tipo': row[1],
                    'fecha': row[2],
                    'numero': row[3],
                    'organismo': row[4],
                    'categoria': row[5]
                }
                projects.append(project_dict)

            return projects

    except Exception as e:
        logging.error(f"Error fetching projects: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)