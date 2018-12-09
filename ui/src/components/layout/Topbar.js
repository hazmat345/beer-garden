import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { withStyles } from '@material-ui/core/styles';
import {
  AppBar,
  Menu,
  MenuItem,
  Toolbar,
  Typography,
  IconButton,
} from '@material-ui/core';
import { AccountCircle } from '@material-ui/icons';

const styles = theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
  },
});

class Topbar extends Component {
  state = {
    anchorEl: null,
  };

  handleMenu = event => {
    this.setState({ anchorEl: event.currentTarget });
  };

  handleClose = event => {
    this.setState({ anchorEl: null });
  };

  render() {
    const { classes } = this.props;
    const { anchorEl } = this.state;
    const open = Boolean(anchorEl);
    return (
      <AppBar position="static" color="primary" className={classes.appBar}>
        <Toolbar>
          <Typography variant="h6" color="inherit" style={{ flex: 1 }}>
            Beer Garden
          </Typography>
          <IconButton
            aria-owns={open ? 'menu-appbar' : undefined}
            aria-haspopup="true"
            onClick={this.handleMenu}
            color="inherit"
          >
            <AccountCircle />
          </IconButton>
          <Menu
            id="menu-appbar"
            anchorEl={anchorEl}
            anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
            transformOrigin={{ vertical: 'top', horizontal: 'right' }}
            open={open}
            onClose={this.handleClose}
          >
            <MenuItem onClick={this.handleClose}>User Settings</MenuItem>
          </Menu>
        </Toolbar>
      </AppBar>
    );
  }
}
Topbar.propTypes = {
  classes: PropTypes.object.isRequired,
};

export default withStyles(styles)(Topbar);
