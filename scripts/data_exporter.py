#!/usr/bin/env python3
"""
Export financial data to various formats (CSV, Excel, HDF5, etc.)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pandas as pd
from pathlib import Path
import json
from loguru import logger

# Configure logger
logger.add("logs/data_export_{time}.log")


def load_parquet_files(pattern):
    """Load all parquet files matching a pattern."""
    data_dir = Path('data')
    files = list(data_dir.rglob(pattern))
    
    if not files:
        logger.warning(f"No files found matching pattern: {pattern}")
        return None
    
    logger.info(f"Found {len(files)} files matching pattern")
    
    # Load and combine data
    dfs = []
    for file in files:
        try:
            df = pd.read_parquet(file)
            # Add source file info
            df['source_file'] = file.name
            dfs.append(df)
            logger.debug(f"Loaded {len(df)} rows from {file.name}")
        except Exception as e:
            logger.error(f"Error loading {file}: {e}")
    
    if dfs:
        combined = pd.concat(dfs, ignore_index=False)
        logger.info(f"Combined data: {len(combined)} total rows")
        return combined
    
    return None


def export_to_csv(data, output_file, index=True):
    """Export DataFrame to CSV."""
    try:
        data.to_csv(output_file, index=index)
        logger.success(f"Exported to CSV: {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        return False


def export_to_excel(data, output_file, sheet_name='data'):
    """Export DataFrame to Excel with formatting."""
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=True)
            
            # Get the worksheet
            worksheet = writer.sheets[sheet_name]
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.success(f"Exported to Excel: {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error exporting to Excel: {e}")
        return False


def export_to_hdf5(data, output_file, key='data'):
    """Export DataFrame to HDF5 format."""
    try:
        data.to_hdf(output_file, key=key, mode='w', complevel=9, complib='blosc')
        logger.success(f"Exported to HDF5: {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error exporting to HDF5: {e}")
        return False


def export_to_json(data, output_file, orient='records'):
    """Export DataFrame to JSON."""
    try:
        # Convert datetime index to string if present
        if isinstance(data.index, pd.DatetimeIndex):
            data_copy = data.copy()
            data_copy.index = data_copy.index.strftime('%Y-%m-%d %H:%M:%S')
            data_copy.to_json(output_file, orient=orient, indent=2)
        else:
            data.to_json(output_file, orient=orient, indent=2)
        
        logger.success(f"Exported to JSON: {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error exporting to JSON: {e}")
        return False


def create_summary_report(data, output_file):
    """Create a summary report of the data."""
    try:
        with open(output_file, 'w') as f:
            f.write("Data Summary Report\n")
            f.write("=" * 50 + "\n\n")
            
            # Basic info
            f.write(f"Total rows: {len(data)}\n")
            f.write(f"Total columns: {len(data.columns)}\n")
            f.write(f"Memory usage: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB\n\n")
            
            # Date range if datetime index
            if isinstance(data.index, pd.DatetimeIndex):
                f.write(f"Date range: {data.index.min()} to {data.index.max()}\n")
                f.write(f"Total days: {(data.index.max() - data.index.min()).days}\n\n")
            
            # Column info
            f.write("Columns:\n")
            for col in data.columns:
                dtype = data[col].dtype
                null_count = data[col].isnull().sum()
                f.write(f"  - {col}: {dtype} ({null_count} nulls)\n")
            
            # Numeric column statistics
            f.write("\nNumeric Column Statistics:\n")
            numeric_cols = data.select_dtypes(include=['number']).columns
            
            for col in numeric_cols:
                f.write(f"\n{col}:\n")
                f.write(f"  Mean: {data[col].mean():.6f}\n")
                f.write(f"  Std: {data[col].std():.6f}\n")
                f.write(f"  Min: {data[col].min():.6f}\n")
                f.write(f"  Max: {data[col].max():.6f}\n")
        
        logger.success(f"Created summary report: {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error creating summary report: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Export financial data to various formats')
    parser.add_argument('--input', required=True, help='Input file pattern (e.g., "*.parquet")')
    parser.add_argument('--output', required=True, help='Output file path')
    parser.add_argument('--format', choices=['csv', 'excel', 'hdf5', 'json'], 
                        default='csv', help='Output format')
    parser.add_argument('--summary', action='store_true', help='Create summary report')
    
    args = parser.parse_args()
    
    # Load data
    logger.info(f"Loading data matching pattern: {args.input}")
    data = load_parquet_files(args.input)
    
    if data is None or data.empty:
        logger.error("No data to export")
        return
    
    # Create output directory if needed
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Export based on format
    success = False
    
    if args.format == 'csv':
        success = export_to_csv(data, args.output)
    elif args.format == 'excel':
        success = export_to_excel(data, args.output)
    elif args.format == 'hdf5':
        success = export_to_hdf5(data, args.output)
    elif args.format == 'json':
        success = export_to_json(data, args.output)
    
    # Create summary report if requested
    if args.summary and success:
        summary_file = output_path.with_suffix('.summary.txt')
        create_summary_report(data, summary_file)
    
    if success:
        logger.info(f"Export completed successfully")
        
        # Print file info
        file_size = output_path.stat().st_size / 1024**2  # MB
        logger.info(f"Output file size: {file_size:.2f} MB")
    else:
        logger.error("Export failed")


if __name__ == "__main__":
    main()