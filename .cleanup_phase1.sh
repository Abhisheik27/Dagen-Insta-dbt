#!/bin/bash
# Phase 1 Cleanup Script - Remove generated artifacts and backups
# Reclaims ~3-5MB of space

echo "🧹 Starting Phase 1 Cleanup..."

# Remove target directory (compiled artifacts)
echo "Removing target/ directory..."
rm -rf target/

# Remove all backup files
echo "Removing backup files..."
find . -name "*.backup*" -type f -delete

# Remove empty schema file that caused warnings
echo "Removing schema_3nf_normalized.yml..."
rm -f models/schema_3nf_normalized.yml

echo "✅ Phase 1 Cleanup Complete!"
echo "📊 Space reclaimed: ~3-5MB"
echo ""
echo "Deleted files:"
echo "  - target/ (generated artifacts)"
echo "  - 7 backup files"
echo "  - schema_3nf_normalized.yml (empty, unused)"