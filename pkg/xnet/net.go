package xnet

import (
	"net"
	"sort"
)

func LookupIP4Addresses(hostname string) ([]string, error) {
	var ipAddresses []string

	// Perform a DNS lookup for the hostname
	ips, err := net.LookupIP(hostname)
	if err != nil {
		return nil, err
	}

	// Collect IPs as strings
	for _, ip := range ips {
		if ip.To4() != nil {
			ipAddresses = append(ipAddresses, ip.String())
		}
	}

	sort.Strings(ipAddresses)
	return ipAddresses, nil
}
