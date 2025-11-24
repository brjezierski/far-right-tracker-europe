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
  experimental: {
    esmExternals: "loose",
  },
};

module.exports = nextConfig;
