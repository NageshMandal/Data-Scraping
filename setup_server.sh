#!/bin/bash
# Server Setup Script for 92GB RAM High-Performance Pipeline
# Usage: chmod +x setup_server.sh && ./setup_server.sh

echo "🔥 Setting up Server-Optimized Job Scraping Pipeline"
echo "=================================================="

# Check if running on server (optional)
TOTAL_RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
if [ "$TOTAL_RAM_GB" -lt 80 ]; then
    echo "⚠️  WARNING: Only ${TOTAL_RAM_GB}GB RAM detected. This setup is optimized for 92GB+ servers."
    read -p "Continue anyway? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "✅ Detected ${TOTAL_RAM_GB}GB RAM - Good for server optimization"

# Install system dependencies
echo "📦 Installing system dependencies..."
if command -v apt-get &> /dev/null; then
    sudo apt-get update
    sudo apt-get install -y python3-dev python3-pip htop iotop
elif command -v yum &> /dev/null; then
    sudo yum install -y python3-devel python3-pip htop iotop
elif command -v pacman &> /dev/null; then
    sudo pacman -S python-pip htop iotop
else
    echo "⚠️  Package manager not detected. Please install python3-dev and monitoring tools manually."
fi

# Install Python dependencies
echo "🐍 Installing Python dependencies..."
pip3 install -r requirements_server.txt

# Set system limits for high-performance processing
echo "⚙️  Configuring system limits..."
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* soft nproc 32768" | sudo tee -a /etc/security/limits.conf  
echo "* hard nproc 32768" | sudo tee -a /etc/security/limits.conf

# Set environment variables for server mode
echo "🌍 Setting up environment variables..."
echo "export SERVER_MODE=true" >> ~/.bashrc
echo "export SERVER_CONFIG_PATH=$(pwd)/config/server_config.json" >> ~/.bashrc

# Create performance monitoring script
echo "📊 Creating performance monitoring script..."
cat > monitor_server.sh << 'EOF'
#!/bin/bash
# Server Performance Monitor for Job Scraping Pipeline

echo "🖥️  Server Performance Monitor"
echo "============================="
echo "Server RAM: $(free -h | awk '/^Mem:/{print $2}')"
echo "Available RAM: $(free -h | awk '/^Mem:/{print $7}')"
echo "CPU Cores: $(nproc)"
echo "Load Average: $(uptime | awk -F'load average:' '{print $2}')"
echo ""

# Check if pipeline is running
if pgrep -f "main_server_optimized" > /dev/null; then
    echo "🚀 Server pipeline is RUNNING"
    
    # Show resource usage
    echo "💾 Memory usage by Python processes:"
    ps aux | grep python | awk '{print $11, $4"% RAM", $3"% CPU"}' | head -5
    
    echo ""
    echo "📈 Top memory consumers:"
    ps aux --sort=-%mem | head -6
    
else
    echo "💤 Server pipeline is NOT running"
fi

echo ""
echo "🔄 To start server pipeline:"
echo "python3 main_server_optimized.py --step full_pipeline"
EOF

chmod +x monitor_server.sh

# Create performance test script
echo "🧪 Creating server performance test..."
cat > test_server_performance.py << 'EOF'
#!/usr/bin/env python3
"""Quick server performance test"""
import psutil
import time
import json

def test_server_specs():
    print("🔍 Server Specification Test")
    print("=" * 40)
    
    # RAM test
    ram_gb = psutil.virtual_memory().total / (1024**3)
    print(f"💾 Total RAM: {ram_gb:.1f}GB")
    
    if ram_gb >= 90:
        print("✅ RAM: Excellent for server optimization")
    elif ram_gb >= 60:
        print("⚠️  RAM: Good, but server config may need adjustment")
    else:
        print("❌ RAM: Insufficient for server optimization")
    
    # CPU test
    cpu_cores = psutil.cpu_count(logical=False)
    cpu_threads = psutil.cpu_count(logical=True)
    print(f"🖥️  CPU: {cpu_cores} cores / {cpu_threads} threads")
    
    if cpu_cores >= 12:
        print("✅ CPU: Excellent for high-concurrency processing")
    elif cpu_cores >= 8:
        print("⚠️  CPU: Good, consider reducing worker counts")
    else:
        print("❌ CPU: May struggle with server configuration")
    
    # Available memory test
    available_gb = psutil.virtual_memory().available / (1024**3)
    print(f"🆓 Available RAM: {available_gb:.1f}GB")
    
    if available_gb >= 70:
        print("✅ Memory: Ready for maximum performance")
    elif available_gb >= 40:
        print("⚠️  Memory: Adequate, monitor during processing")
    else:
        print("❌ Memory: Free up memory before running server pipeline")
    
    # Disk test
    disk = psutil.disk_usage('/')
    disk_free_gb = disk.free / (1024**3)
    print(f"💽 Free disk space: {disk_free_gb:.1f}GB")
    
    if disk_free_gb >= 50:
        print("✅ Disk: Sufficient space")
    else:
        print("⚠️  Disk: Consider freeing disk space")
    
    print("\n🚀 Server optimization recommendation:")
    if ram_gb >= 90 and cpu_cores >= 12 and available_gb >= 70:
        print("✅ EXCELLENT - Use full server configuration")
        print("   Run: python3 main_server_optimized.py --step full_pipeline")
    elif ram_gb >= 60 and cpu_cores >= 8:
        print("⚠️  GOOD - Reduce server configuration by 25%")
        print("   Edit config/server_config.json and reduce worker counts")
    else:
        print("❌ LIMITED - Use M1 Pro configuration instead")
        print("   Run: python3 main_optimized.py --step full_pipeline")

if __name__ == "__main__":
    test_server_specs()
EOF

chmod +x test_server_performance.py

# Validate server configuration
echo "🔍 Validating server setup..."
python3 test_server_performance.py

echo ""
echo "🎉 Server Setup Complete!"
echo "======================="
echo ""
echo "📋 Next steps:"
echo "1. Copy your .env file to this directory"
echo "2. Test the setup: python3 main_server_optimized.py --validate"
echo "3. Monitor performance: ./monitor_server.sh"
echo "4. Run server pipeline: python3 main_server_optimized.py --step full_pipeline"
echo ""
echo "🔧 Available commands:"
echo "  ./monitor_server.sh                    # Monitor server performance"
echo "  python3 test_server_performance.py    # Test server specifications"
echo "  python3 main_server_optimized.py --validate  # Validate configuration"
echo "  python3 main_server_optimized.py --stats     # Show pipeline statistics"
echo ""
echo "📚 Documentation: SERVER_OPTIMIZATION_README.md" 