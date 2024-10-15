# SemT-Geonames
Genomes for data Enrichment in the SemT framework

## Environment Setup

1. Copy the `.env.example` file to a new file named `.env`:
   ```
   cp .env.example .env
   ```

2. Open the `.env` file and replace the placeholder values with your actual configuration:
   - Set `MONGO_USERNAME` and `MONGO_PASSWORD` to your desired MongoDB credentials
   - Adjust other variables as needed for your environment

3. Save the `.env` file. Docker Compose will automatically use these environment variables when you run your containers.

Note: The `.env` file contains sensitive information and should never be committed to version control. It is already included in the `.gitignore` file.