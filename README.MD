# Go Migration App

A Go application designed to handle database migration tasks between PostgreSQL databases, with specific functionality for user data, BDM (Business Development Manager), Wukala (Travel Agent), and persona data migration.

## Project Structure

```
go-migration-app/
├── config/
│   └── config.go
├── database/
│   └── connectionDB.go
├── service/
│   ├── bdm.go
│   ├── bdm-persona.go
│   ├── change-city-id-persona.go
│   ├── checking-wukala.go
│   ├── travel.go
│   ├── user.go
│   ├── user-persona.go
│   └── wukala-persona.go
└── main.go
```

## Prerequisites

- Go 1.x or higher
- PostgreSQL
- Environment variables configured in `.env` file

## Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
SOURCE_DB_HOST=your_source_host
SOURCE_DB_PORT=your_source_port
SOURCE_DB_NAME=your_source_db_name
SOURCE_DB_USER=your_source_username
SOURCE_DB_PASSWORD=your_source_password

TARGET_DB_HOST=your_target_host
TARGET_DB_PORT=your_target_port
TARGET_DB_NAME=your_target_db_name
TARGET_DB_USER=your_target_username
TARGET_DB_PASSWORD=your_target_password
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/ApesJs/go-migration-app.git
```

2. Install dependencies:
```bash
go mod tidy
```

## Features

### 1. User Migration
- Transfers user data from source to target database
- Handles role assignments (including Wukala role)
- Manages duplicate email detection
- Provides detailed transfer statistics

### 2. BDM (Business Development Manager) Migration
- Transfers BDM-specific data
- Creates and assigns BDM roles
- Handles user-role associations

### 3. Wukala (Travel Agent) Migration
- Validates and transfers travel agent data
- Manages travel agent-specific attributes
- Handles parent-child relationships

### 4. Persona Data Migration
- Transfers user persona information
- Handles duplicate phone numbers
- Manages additional user attributes (address, job, DOB, etc.)

### 5. City ID Migration
- Converts city IDs to city names
- Updates user persona records
- Maintains data integrity during conversion

## Available Services

1. `UserService()`: Handles base user data migration
2. `BDMService()`: Manages BDM data transfer
3. `BdmPersonaService()`: Handles BDM persona information
4. `WukalaPersonaService()`: Manages travel agent persona data
5. `ChangeCityIDPersonaService()`: Converts and updates city information
6. `CheckingWukalaService()`: Validates Wukala data consistency
7. `TravelService()`: Manages travel-related data migration
8. `UserPersonaService()`: Handles general user persona information

## Usage

To use specific migration services, uncomment the desired service in `main.go`:

```go
func main() {
    // Uncomment the service you want to run
    //service.UserService()
    //service.BDMService()
    //service.BdmPersonaService()
    service.ChangeCityIDPersonaService()
}
```

## Progress Tracking

The application provides real-time progress tracking with:
- Progress bar visualization
- Transfer statistics
- Error reporting
- Duplicate entry detection
- Performance metrics

## Error Handling

The application includes comprehensive error handling for:
- Database connection issues
- Duplicate records
- Data validation
- Transaction management
- Invalid data types

## Performance Features

- Transaction-based operations
- Batch processing
- Prepared statements
- Connection pooling
- Progress monitoring

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details