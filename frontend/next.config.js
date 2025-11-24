/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === 'production';

const nextConfig = {
  output: "export",
  basePath: isProd ? "/far-right-tracker-europe" : "",
  assetPrefix: isProd ? "/far-right-tracker-europe" : "",
  images: {
    unoptimized: true,
  },
  reactStrictMode: true,
  typescript: {
    // Dangerously allow production builds to successfully complete even if type errors
    // This prevents build failures from internal Next.js type resolution issues
    ignoreBuildErrors: false,
  },
  eslint: {
    ignoreDuringBuilds: false,
  },
};

module.exports = nextConfig;
