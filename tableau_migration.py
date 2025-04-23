#!/usr/bin/env python3
"""
Tableau Server Migration Tool

This script allows migrating workbooks from one Tableau server to another,
supporting different sites and folder structures.
"""

import os
import sys
import argparse
import getpass
import logging
import tempfile
import tableauserverclient as TSC
from pathlib import Path


class TableauMigrator:
    def __init__(self, source_server, target_server, source_site, target_site, 
                 logger=None, source_token_name=None, source_token_value=None, 
                 target_token_name=None, target_token_value=None,
                 source_username=None, source_password=None, 
                 target_username=None, target_password=None):
        
        self.source_server_url = source_server
        self.target_server_url = target_server
        self.source_site = source_site
        self.target_site = target_site
        
        # Authentication info
        self.source_token_name = source_token_name
        self.source_token_value = source_token_value
        self.target_token_name = target_token_name
        self.target_token_value = target_token_value
        self.source_username = source_username
        self.source_password = source_password
        self.target_username = target_username
        self.target_password = target_password
        
        # Server connections
        self.source_server = None
        self.target_server = None
        
        # Set up logging
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('tableau_migrator')
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Temp directory for downloaded workbooks
        self.temp_dir = tempfile.mkdtemp()
        self.logger.info(f"Created temporary directory: {self.temp_dir}")

    def connect_to_source(self):
        """Connect to the source Tableau server"""
        self.logger.info(f"Connecting to source server: {self.source_server_url}, site: {self.source_site}")
        
        if self.source_token_name and self.source_token_value:
            auth = TSC.PersonalAccessTokenAuth(
                token_name=self.source_token_name,
                personal_access_token=self.source_token_value,
                site_id=self.source_site
            )
            self.logger.info(f"Using token authentication for source server")
        elif self.source_username:
            password = self.source_password or getpass.getpass("Source Server Password: ")
            auth = TSC.TableauAuth(self.source_username, password, site_id=self.source_site)
            self.logger.info(f"Using username/password authentication for source server")
        else:
            raise ValueError("No authentication credentials provided for source server")
        
        self.source_server = TSC.Server(self.source_server_url, use_server_version=True)
        self.source_server.auth.sign_in(auth)
        self.logger.info(f"Successfully connected to source server")
        return self.source_server

    def connect_to_target(self):
        """Connect to the target Tableau server"""
        self.logger.info(f"Connecting to target server: {self.target_server_url}, site: {self.target_site}")
        
        if self.target_token_name and self.target_token_value:
            auth = TSC.PersonalAccessTokenAuth(
                token_name=self.target_token_name,
                personal_access_token=self.target_token_value,
                site_id=self.target_site
            )
            self.logger.info(f"Using token authentication for target server")
        elif self.target_username:
            password = self.target_password or getpass.getpass("Target Server Password: ")
            auth = TSC.TableauAuth(self.target_username, password, site_id=self.target_site)
            self.logger.info(f"Using username/password authentication for target server")
        else:
            raise ValueError("No authentication credentials provided for target server")
        
        self.target_server = TSC.Server(self.target_server_url, use_server_version=True)
        self.target_server.auth.sign_in(auth)
        self.logger.info(f"Successfully connected to target server")
        return self.target_server

    def list_source_sites(self):
        """List all sites on the source server"""
        if not self.source_server:
            self.connect_to_source()
        
        all_sites = list(TSC.Pager(self.source_server.sites))
        self.logger.info(f"Found {len(all_sites)} sites on source server")
        return all_sites
    
    def list_projects(self, server, site=None):
        """List all projects on a server/site"""
        if site and server.site_id != site:
            # Switch to the specified site if needed
            current_site = server.site_id
            self.logger.info(f"Switching from site {current_site} to {site}")
            server.auth.switch_site(site)
        
        all_projects = list(TSC.Pager(server.projects))
        self.logger.info(f"Found {len(all_projects)} projects on site {server.site_id}")
        return all_projects
    
    def list_workbooks(self, server, site=None, project_id=None):
        """List all workbooks on a server/site, optionally filtered by project"""
        if site and server.site_id != site:
            # Switch to the specified site if needed
            current_site = server.site_id
            self.logger.info(f"Switching from site {current_site} to {site}")
            server.auth.switch_site(site)
        
        # If project_id is provided, filter by project
        req_option = TSC.RequestOptions()
        if project_id:
            req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectId, 
                                            TSC.RequestOptions.Operator.Equals, 
                                            project_id))
        
        all_workbooks = list(TSC.Pager(server.workbooks, req_option))
        self.logger.info(f"Found {len(all_workbooks)} workbooks on site {server.site_id}" + 
                        (f" in project {project_id}" if project_id else ""))
        return all_workbooks
    
    def ensure_project_exists(self, project_name, parent_id=None):
        """Make sure a project exists on the target server, create if it doesn't"""
        # Check if project exists
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, 
                                          TSC.RequestOptions.Operator.Equals, 
                                          project_name))
        
        matching_projects = list(TSC.Pager(self.target_server.projects, req_option))
        
        if matching_projects:
            for project in matching_projects:
                # If parent_id is None, we're looking for top-level project
                # If parent_id is not None, we need to match it
                if (parent_id is None and project.parent_id is None) or \
                   (parent_id is not None and project.parent_id == parent_id):
                    self.logger.info(f"Found existing project: {project_name}")
                    return project
        
        # Create the project if it doesn't exist
        new_project = TSC.ProjectItem(name=project_name, parent_id=parent_id)
        new_project = self.target_server.projects.create(new_project)
        self.logger.info(f"Created new project: {project_name}")
        return new_project
    
    def migrate_workbook(self, workbook_id, source_project, target_project_id):
        """Migrate a single workbook from source to target"""
        if not self.source_server:
            self.connect_to_source()
        if not self.target_server:
            self.connect_to_target()
            
        # Download the workbook
        workbook_file = os.path.join(self.temp_dir, f"workbook_{workbook_id}.twbx")
        self.logger.info(f"Downloading workbook {workbook_id} to {workbook_file}")
        self.source_server.workbooks.download(workbook_id, workbook_file)
        
        # Get workbook details for name and other metadata
        workbook = self.source_server.workbooks.get_by_id(workbook_id)
        
        # Create a new workbook item with the target project id
        new_workbook = TSC.WorkbookItem(project_id=target_project_id, name=workbook.name)
        
        # Upload to target
        self.logger.info(f"Uploading workbook {workbook.name} to target project {target_project_id}")
        self.target_server.workbooks.publish(new_workbook, workbook_file, 'Overwrite')
        
        # Clean up the temp file
        os.remove(workbook_file)
        self.logger.info(f"Successfully migrated workbook {workbook.name}")
    
    def migrate_project(self, source_project_id, target_project_id=None):
        """Migrate all workbooks from a source project to a target project"""
        if not self.source_server:
            self.connect_to_source()
        if not self.target_server:
            self.connect_to_target()
        
        # Get source project details
        source_project = self.source_server.projects.get_by_id(source_project_id)
        
        # If no target project ID is provided, create or find a matching project
        if not target_project_id:
            target_project = self.ensure_project_exists(source_project.name, source_project.parent_id)
            target_project_id = target_project.id
        
        # Get all workbooks in the source project
        workbooks = self.list_workbooks(self.source_server, project_id=source_project_id)
        
        # Migrate each workbook
        for workbook in workbooks:
            self.migrate_workbook(workbook.id, source_project, target_project_id)
        
        self.logger.info(f"Successfully migrated {len(workbooks)} workbooks from project {source_project.name}")
    
    def migrate_site(self, source_site_id=None, target_site_id=None):
        """Migrate all projects and workbooks from a source site to a target site"""
        # Use current site if none specified
        source_site_id = source_site_id or self.source_site
        target_site_id = target_site_id or self.target_site
        
        # Ensure we're connected to both servers
        if not self.source_server:
            self.connect_to_source()
        if not self.target_server:
            self.connect_to_target()
        
        # Switch to the specified sites if needed
        if self.source_server.site_id != source_site_id:
            self.source_server.auth.switch_site(source_site_id)
        
        if self.target_server.site_id != target_site_id:
            self.target_server.auth.switch_site(target_site_id)
        
        # Get all projects in the source site
        source_projects = self.list_projects(self.source_server)
        
        # Create project hierarchy mapping
        project_map = {}
        
        # First pass: create all top-level projects
        for project in source_projects:
            if not project.parent_id:
                target_project = self.ensure_project_exists(project.name)
                project_map[project.id] = target_project.id
        
        # Second pass: create all child projects 
        # This might need multiple passes for deep hierarchies
        remaining_projects = [p for p in source_projects if p.parent_id]
        while remaining_projects:
            projects_handled = []
            for project in remaining_projects:
                if project.parent_id in project_map:
                    # Parent has been created, so we can create this one
                    target_parent_id = project_map[project.parent_id]
                    target_project = self.ensure_project_exists(project.name, target_parent_id)
                    project_map[project.id] = target_project.id
                    projects_handled.append(project)
            
            if not projects_handled:
                # If we didn't handle any projects in this pass, we have an issue
                self.logger.error(f"Unable to create project hierarchy for {len(remaining_projects)} projects")
                break
                
            # Remove handled projects from the remaining list
            remaining_projects = [p for p in remaining_projects if p not in projects_handled]
        
        # Now migrate all projects
        for source_project_id, target_project_id in project_map.items():
            self.migrate_project(source_project_id, target_project_id)
        
        self.logger.info(f"Successfully migrated site {source_site_id} to {target_site_id}")
    
    def cleanup(self):
        """Clean up temporary files and sign out of servers"""
        # Clean up temp directory
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            self.logger.info(f"Removed temporary directory: {self.temp_dir}")
        
        # Sign out of servers
        if self.source_server:
            self.source_server.auth.sign_out()
            self.logger.info("Signed out of source server")
        
        if self.target_server:
            self.target_server.auth.sign_out()
            self.logger.info("Signed out of target server")


def main():
    parser = argparse.ArgumentParser(description="Migrate workbooks between Tableau servers")
    
    # Server connection options
    parser.add_argument("--source-server", "-ss", required=True,
                        help="Source Tableau server URL (e.g., https://tableau.example.com)")
    parser.add_argument("--target-server", "-ts", 
                        help="Target Tableau server URL (e.g., https://tableau-target.example.com)")
    parser.add_argument("--source-site", "-ssite", default="",
                        help="Source site ID (use empty string for default site)")
    parser.add_argument("--target-site", "-tsite", default="",
                        help="Target site ID (use empty string for default site)")
    
    # Authentication options - Source
    source_auth = parser.add_argument_group("Source Server Authentication")
    source_auth_method = source_auth.add_mutually_exclusive_group(required=True)
    source_auth_method.add_argument("--source-token-name", "-stn",
                                  help="Name of personal access token for source server")
    source_auth_method.add_argument("--source-username", "-su",
                                  help="Username for source server")
    source_auth.add_argument("--source-token-value", "-stv",
                           help="Value of personal access token for source server")
    source_auth.add_argument("--source-password", "-sp",
                           help="Password for source server")
    
    # Authentication options - Target
    target_auth = parser.add_argument_group("Target Server Authentication")
    target_auth_method = target_auth.add_mutually_exclusive_group(required=False)
    target_auth_method.add_argument("--target-token-name", "-ttn",
                                  help="Name of personal access token for target server")
    target_auth_method.add_argument("--target-username", "-tu",
                                  help="Username for target server")
    target_auth.add_argument("--target-token-value", "-ttv",
                           help="Value of personal access token for target server")
    target_auth.add_argument("--target-password", "-tp",
                           help="Password for target server")
    
    # Action to perform
    action = parser.add_argument_group("Migration Action")
    action_type = action.add_mutually_exclusive_group(required=True)
    action_type.add_argument("--list-sites", action="store_true",
                           help="List available sites on source server")
    action_type.add_argument("--list-projects", action="store_true",
                           help="List available projects on source site")
    action_type.add_argument("--list-workbooks", action="store_true",
                           help="List available workbooks on source site")
    action_type.add_argument("--migrate-workbook", "-mw",
                           help="ID of workbook to migrate")
    action_type.add_argument("--migrate-project", "-mp",
                           help="ID of project to migrate")
    action_type.add_argument("--migrate-site", action="store_true",
                           help="Migrate entire site")
    
    # Additional options
    parser.add_argument("--source-project-id", "-spid",
                      help="Source project ID (required for --list-workbooks)")
    parser.add_argument("--target-project-id", "-tpid",
                      help="Target project ID (optional for --migrate-workbook and --migrate-project)")
    parser.add_argument("--verbosity", "-v", choices=["debug", "info", "warning", "error"],
                      default="info", help="Logging verbosity")
    
    args = parser.parse_args()
    
    # Check that target server is provided for migration operations
    if (args.migrate_workbook or args.migrate_project or args.migrate_site) and not args.target_server:
        parser.error("--target-server is required for migration operations")
    
    # Check that target authentication is provided for migration operations
    if (args.migrate_workbook or args.migrate_project or args.migrate_site) and not (args.target_token_name or args.target_username):
        parser.error("Target server authentication (--target-token-name or --target-username) is required for migration operations")
    
    # Set up logging
    logging_level = getattr(logging, args.verbosity.upper())
    logger = logging.getLogger('tableau_migrator')
    logger.setLevel(logging_level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Create migrator
    migrator = TableauMigrator(
        source_server=args.source_server,
        target_server=args.target_server,
        source_site=args.source_site,
        target_site=args.target_site,
        logger=logger,
        source_token_name=args.source_token_name,
        source_token_value=args.source_token_value,
        target_token_name=args.target_token_name,
        target_token_value=args.target_token_value,
        source_username=args.source_username,
        source_password=args.source_password,
        target_username=args.target_username,
        target_password=args.target_password
    )
    
    try:
        # Execute requested action
        if args.list_sites:
            sites = migrator.list_source_sites()
            print("\nAvailable sites on source server:")
            for site in sites:
                print(f"  - {site.name} (ID: {site.id}, URL: {site.content_url})")
        
        elif args.list_projects:
            migrator.connect_to_source()
            projects = migrator.list_projects(migrator.source_server)
            print("\nAvailable projects on source site:")
            for project in projects:
                parent = f" (Parent ID: {project.parent_id})" if project.parent_id else ""
                print(f"  - {project.name} (ID: {project.id}){parent}")
        
        elif args.list_workbooks:
            migrator.connect_to_source()
            workbooks = migrator.list_workbooks(migrator.source_server, 
                                               project_id=args.source_project_id)
            print("\nAvailable workbooks:")
            for workbook in workbooks:
                print(f"  - {workbook.name} (ID: {workbook.id})")
        
        elif args.migrate_workbook:
            if not args.source_project_id:
                logger.error("--source-project-id is required when using --migrate-workbook")
                sys.exit(1)
            migrator.connect_to_source()
            migrator.connect_to_target()
            
            # If target project not specified, use same structure as source
            target_project_id = args.target_project_id
            if not target_project_id:
                source_project = migrator.source_server.projects.get_by_id(args.source_project_id)
                target_project = migrator.ensure_project_exists(source_project.name)
                target_project_id = target_project.id
                
            migrator.migrate_workbook(args.migrate_workbook, args.source_project_id, target_project_id)
        
        elif args.migrate_project:
            migrator.connect_to_source()
            migrator.connect_to_target()
            migrator.migrate_project(args.migrate_project, args.target_project_id)
        
        elif args.migrate_site:
            migrator.migrate_site()
    
    finally:
        # Only clean up source server for listing operations
        if args.list_sites or args.list_projects or args.list_workbooks:
            if migrator.source_server:
                migrator.source_server.auth.sign_out()
                migrator.logger.info("Signed out of source server")
        else:
            # Full cleanup for migration operations
            migrator.cleanup()


if __name__ == "__main__":
    main() 
