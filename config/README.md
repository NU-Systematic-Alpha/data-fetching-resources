# Configuration Setup

## Setting up API Keys

1. **Copy the example configuration file:**
   ```bash
   cp config.ini.example config.ini
   ```

2. **Get your Polygon.io API key:**
   - Sign up at [polygon.io](https://polygon.io/)
   - Navigate to your dashboard
   - Copy your API key
   - Replace `YOUR_POLYGON_API_KEY_HERE` in `config.ini` with your actual key

## Configuration Options

### API Keys
- `polygon.api_key`: Your Polygon.io API key

### Paths
- `data_dir`: Directory where downloaded data will be stored (default: `data/`)
- `cache_dir`: Directory for cached API responses (default: `cache/`)

### Rate Limits
- `polygon_rpm`: Requests per minute based on your Polygon subscription tier
  - Free: 5 requests/minute
  - Basic: 100 requests/minute
  - Starter and above: unlimited

### Cache Settings
- `stock_data_ttl`: Time to live for cached stock data (seconds)
- `options_data_ttl`: Time to live for cached options data (seconds)
- `treasury_data_ttl`: Time to live for cached treasury data (seconds)

## Security Notes

- **NEVER** commit your actual `config.ini` file to version control
- The `.gitignore` file is configured to exclude `config.ini`
- Keep your API keys secure and rotate them regularly
- Consider using environment variables for production deployments