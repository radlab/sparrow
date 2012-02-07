import boto
import os
import sys
import tempfile
import time
import subprocess
import shutil

from optparse import OptionParser


def parse_args():
  parser = OptionParser(usage="sparrow-exp <action> [options]" +
    "\n\n<action> can be: launch, start, stop, start-proto, stop-proto")
  parser.add_option("-z", "--zone", default="us-east-1b",
      help="Availability zone to launch instances in")
  parser.add_option("-a", "--ami", default="ami-83c717ea",
      help="Amazon Machine Image ID to use")
  parser.add_option("-t", "--instance-type", default="m1.large",
      help="Type of instance to launch (default: m1.large). " +
           "WARNING: must be 64 bit, thus small instances won't work")
  parser.add_option("-k", "--key-pair",
      help="Key pair to use on instances")
  parser.add_option("-i", "--identity-file",
      help="SSH private key file to use for logging into instances")
  parser.add_option("-f", "--frontends", type="int", default=1,
      help="Number of frontends to launch (default: 1)")
  parser.add_option("-b", "--backends", type="int", default=1,
      help="Number of backends to launch (default: 1)")
  parser.add_option("-w", "--wait", type="int", default=0,
      help="Number of seconds to wait for cluster nodes to boot (default: 0)")

  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)
  if os.getenv('AWS_ACCESS_KEY_ID') == None:
    print >> sys.stderr, ("ERROR: The environment variable AWS_ACCESS_KEY_ID " +
                          "must be set")
    sys.exit(1)
  if os.getenv('AWS_SECRET_ACCESS_KEY') == None:
    print >> sys.stderr ("ERROR: The environment variable " +
                         "AWS_SECRET_ACCESS_KEY must be set")
    sys.exit(1)

  return (opts, args)

# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name):
  groups = conn.get_all_security_groups()
  group = [g for g in groups if g.name == name]
  if len(group) > 0:
    return group[0]
  else:
    print "Creating security group " + name
    return conn.create_security_group(name, "Sparrow EC2 group")

# Copy a file to a given host through scp, throwing an exception if scp fails
def scp(host, opts, local_file, dest_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' 'root@%s:%s'" %
      (opts.identity_file, local_file, host, dest_file), shell=True)

# Run a command on a host through ssh, throwing an exception if ssh fails
def ssh(host, opts, command):
  subprocess.check_call(
      "ssh -t -o StrictHostKeyChecking=no -i %s root@%s '%s'" %
      (opts.identity_file, host, command), shell=True)

# Run a command on multiple hosts through ssh, throwing an exception on failure
def ssh_all(hosts, opts, commands):
  for h in hosts:
    print h
    ssh(h, opts, commands)

# Launch a cluster and return instances launched
def launch_cluster(conn, opts):
  backend_group = get_or_make_group(conn, "sparrow-backends")
  frontend_group = get_or_make_group(conn, "sparrow-frontends")
  groups = [backend_group, frontend_group]

  for group in groups:
    if group.rules == []: # Group was now just created
      # Allow all access from all other sparrow machines
      for group2 in groups:
        group.authorize(src_group=group2) 
      # Allow some access from all machines
      group.authorize('tcp', 22, 22, '0.0.0.0/0')

  print "Launching instances..."
  try:
    image = conn.get_all_images(image_ids=[opts.ami])[0]
  except:
    print >> stderr, "Could not find AMI " + opts.ami
    sys.exit(1)

  frontend_res = image.run(key_name = opts.key_pair,
                          security_groups = [frontend_group],
                          instance_type = opts.instance_type,
                          placement = opts.zone,
                          min_count = opts.frontends,
                          max_count = opts.frontends)
  backend_res = image.run(key_name = opts.key_pair,
                          security_groups = [backend_group],
                          instance_type = opts.instance_type,
                          placement = opts.zone,
                          min_count = opts.backends,
                          max_count = opts.backends)

  print "Launched cluster with %s frontends and %s backends" % (
         opts.frontends, opts.backends)

  return(frontend_res.instances, backend_res.instances)

# Wait for a set of launched instances to exit the "pending" state
# (i.e. either to start running or to fail and be terminated)
def wait_for_instances(conn, instances):
  while True:
    for i in instances:
      i.update()
    if len([i for i in instances if i.state == 'pending']) > 0:
      time.sleep(5)
    else:
      return

# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])


def find_existing_cluster(conn, opts):
  print "Searching for existing Sparrow cluster..."
  reservations = conn.get_all_instances()
  frontend_nodes = []
  backend_nodes = []
  for res in reservations:
    active = [i for i in res.instances if is_active(i)]
    if len(active) > 0:
      group_names = [g.name for g in res.groups]
      if group_names == ["sparrow-frontends"]:
        frontend_nodes += res.instances
      elif group_names == ["sparrow-backends"]:
        backend_nodes += res.instances
  if frontend_nodes != [] and backend_nodes != []:
    print ("Found %d frontend and %s backend nodes" %
           (len(frontend_nodes), len(backend_nodes)))

    print "Frontends:"
    for fe in frontend_nodes:
      print fe.public_dns_name
    print "Backends:"
    for be in backend_nodes:
      print be.public_dns_name

    return (frontend_nodes, backend_nodes)
  else:
    print "ERROR: Could not find full cluster: fe=%s be=%s" % (
      frontend_nodes, backend_nodes)
    sys.exit(1)


# Deploy Sparrow binaries and configuration on a launched cluster
def deploy_cluster(frontends, backends, opts):
  # Replace template vars
  tmp_dir = tempfile.mkdtemp()

  template_vars = {
    "static_frontends": ",".join(["%s:12345" % i.public_dns_name \
                                 for i in frontends]),
    "frontend_list": "\n".join(["%s" % i.public_dns_name \
                                 for i in frontends]),
    "static_backends": ",".join(["%s:20502" % i.public_dns_name \
                                 for i in backends]),
    "backend_list": "\n".join(["%s" % i.public_dns_name \
                                 for i in backends])
  }
  for filename in os.listdir("template"):
    if filename[0] not in '#.~' and filename[-1] != '~':
      local_file = os.path.join(tmp_dir, filename)
      with open(os.path.join("template", filename)) as src:
        with open(local_file, "w") as dest:
          text = src.read()
          for key in template_vars:
            text = text.replace("{{" + key + "}}", template_vars[key])
          dest.write(text)
          dest.close()

  driver_machine = frontends[0].public_dns_name
  print "Chose driver machine: %s ..." % driver_machine
 
  # Rsync this to one machine
  command = (("rsync -rv -e 'ssh -o StrictHostKeyChecking=no -i %s' " +
      "'%s/' 'root@%s:~/'") % (opts.identity_file, tmp_dir, driver_machine))
  subprocess.check_call(command, shell=True)
  # Remove the temp directory we created above
  shutil.rmtree(tmp_dir)

  print "Copying SSH key %s to driver..." % opts.identity_file
  ssh(driver_machine, opts, 'mkdir -p /root/.ssh')
  scp(driver_machine, opts, opts.identity_file, '/root/.ssh/id_rsa')

  print "Building sparrow on driver machine..."
  ssh(driver_machine, opts, "chmod 775 /root/build_sparrow.sh;" + 
                            "/root/build_sparrow.sh;")

  print "Deploying sparrow to other machines..."
  ssh(driver_machine, opts, "chmod 775 /root/deploy_sparrow.sh;" +
                            "/root/deploy_sparrow.sh")  

# Start a deployed Sparrow cluster
def start_cluster(frontends, backends, opts):
  all_machines = []
  for fe in frontends: 
    all_machines.append(fe.public_dns_name) 
  for be in backends: 
    all_machines.append(be.public_dns_name)

  print "Starting sparrow on all machines..."
  ssh_all(all_machines, opts, "chmod 755 /root/start_sparrow.sh;" +
                              "/root/start_sparrow.sh;")

# Stop a deployed Sparrow cluster
def stop_cluster(frontends, backends, opts):
  all_machines = []
  for fe in frontends:
    all_machines.append(fe.public_dns_name)
  for be in backends:
    all_machines.append(be.public_dns_name)
  print "Stopping sparrow on all machines..."
  ssh_all(all_machines, opts, "chmod 755 /root/stop_sparrow.sh;" +
                              "/root/stop_sparrow.sh;")

# Start the prototype backends/frontends
def start_proto(frontends, backends, opts):
  print "Starting Proto backends..."
  ssh_all([be.public_dns_name for be in backends], opts,
         "chmod 755 /root/start_proto_backend.sh;" + 
         "/root/start_proto_backend.sh")
  print "Starting Proto frontends..."
  ssh_all([fe.public_dns_name for fe in frontends], opts,
          "chmod 755 /root/start_proto_frontend.sh;" + 
          "/root/start_proto_frontend.sh")

# Start the prototype backends/frontends
def stop_proto(frontends, backends, opts):
  print "Stopping Proto backends..."
  ssh_all([be.public_dns_name for be in backends], opts,
         "chmod 755 /root/stop_proto_backend.sh;" +
         "/root/stop_proto_backend.sh")
  print "Stopping Proto frontends..."
  ssh_all([fe.public_dns_name for fe in frontends], opts,
          "chmod 755 /root/stop_proto_frontend.sh;" +
          "/root/stop_proto_frontend.sh")


def main():
  (opts, args) = parse_args()
  conn = boto.connect_ec2()
  action = args[0]

  if action == "launch":
    (frontends, backends) = launch_cluster(conn, opts)
    return

  # Wait until ec2 says the cluster is started, then possibly wait more time
  # to make sure all nodes have booted.
  (frontends, backends) = find_existing_cluster(conn, opts)
  print "Waiting for instances to start up"
  wait_for_instances(conn, frontends)
  wait_for_instances(conn, backends)

  print "Waiting %d more seconds..." % opts.wait
  time.sleep(opts.wait)

  if action == "deploy":
    print "Deploying files..."
    deploy_cluster(frontends, backends, opts)

  if action == "start":
    print "Starting cluster..."
    start_cluster(frontends, backends, opts)

  if action == "stop":
    print "Stopping cluster..."
    stop_cluster(frontends, backends, opts)

  if action == "start-proto":
    print "Starting proto application..."
    start_proto(frontends, backends, opts)

  if action == "stop-proto":
    print "Stopping proto application..."
    stop_proto(frontends, backends, opts)

if __name__ == "__main__":
  main()
